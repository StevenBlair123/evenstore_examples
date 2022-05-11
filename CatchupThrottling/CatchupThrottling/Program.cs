using System;

namespace CatchupThrottling {
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using EventStore.Client;

    public record StreamState {
        public Int32 Id { get; init; }
        public Int64 EventsProcessed { get; init; }
        public DateTime DateTimeOfLastEvent { get; set; }
        public Int32 NumberOfTimeouts{ get; set; }
    }

    class Program {
        private static Dictionary<Int32, StreamState> streamStats = new();
        private static Int64 totalEventsProcessed;

        static async Task Main(String[] args) {
            String connection = "esdb://admin:changeit@192.168.10.90:2113?keepAliveInterval=10000&keepAliveTimeout=10000&tls=true&tlsVerifyCert=false";
            String stream = "$ce-SalesTransactionAggregate";
            Int32 numberOfCatchups = 15;

            for (Int32 i = 0; i < numberOfCatchups; i++) {
                StreamState state = new() {
                                              EventsProcessed = 0,
                                              Id = i + 1
                                          };

                streamStats.Add(i + 1, state);

                await CatchupTest(connection, stream, state);
            }

            Console.ReadKey();
        }

        static async Task CatchupTest(String connectionString, String stream, StreamState state) {
            EventStoreClientSettings settings = EventStoreClientSettings.Create($"{connectionString}");
            EventStoreClient client = new (settings);

            await client.SubscribeToStreamAsync(stream,
                                                FromStream.Start,
                                                async (sub, @event, can) => await EventAppeared(sub, @event, state.Id, can),
                                                true,
                                                SubscriptionDropped);
        }

        private static void SubscriptionDropped(StreamSubscription arg1, SubscriptionDroppedReason arg2, Exception? arg3) =>
            Console.WriteLine($"Subscription dropped");

        private static async Task EventAppeared(StreamSubscription arg1, ResolvedEvent arg2, Int32 id, CancellationToken arg3) {
            StreamState state = streamStats[id];
            Interlocked.Increment(ref totalEventsProcessed);

            //Some processing time.
            Random random = new(state.GetHashCode());
            Int32 val = random.Next(0, 10000);
            Boolean timeout = val switch {
                0 => true,
                _ => false
            };

            if (timeout){
                Console.WriteLine($"Timeout occurred for Id [{state.Id}]");

                state = state with
                        {
                            NumberOfTimeouts = state.NumberOfTimeouts + 1
                        };
            }

            state = state with
                    {
                        DateTimeOfLastEvent = DateTime.Now,
                        EventsProcessed = state.EventsProcessed + 1
                    };

            streamStats[id] = state; //overwrite

            if (timeout){
                //This event should timeout.
                await Task.Delay(TimeSpan.FromSeconds(30), arg3);
            }

            if ((totalEventsProcessed % 50000) == 0) {
                //Dump out report
                streamStats.ToList()
                           .ForEach(x => Console.WriteLine($"Stream Id [{x.Value.Id}] has processed [{x.Value.EventsProcessed}] events. Last processed [{x.Value.DateTimeOfLastEvent}]. Number of timeouts [{x.Value.NumberOfTimeouts}]"));
            }
        }
    }
}
