using System;

namespace CatchupThrottling {
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;
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
            //connection = "esdb://admin:changeit@192.168.10.90:2113?tls=true&tlsVerifyCert=false";
            String stream = "$ce-SalesTransactionAggregate";
            Int32 numberOfCatchups = 25;
            Int32 numberOfConcurrentWriters = 50;

            EventStoreClientSettings settings = EventStoreClientSettings.Create($"{connection}");
            EventStoreClient client = new(settings);

            for (Int32 i = 0; i < numberOfCatchups; i++) {
                StreamState state = new() {
                                              EventsProcessed = 0,
                                              Id = i + 1
                                          };

                streamStats.Add(i + 1, state);

                await CatchupTest(client, stream, state);
            }

            ActionBlock<String> actionBlock = new(async streamName =>
                                                  {
                                                      try{
                                                          List<EventData> events = GetEvents(10);
                                                          await client.AppendToStreamAsync(streamName, StreamRevision.None, events);
                                                      }
                                                      catch(Exception e){
                                                          Console.WriteLine(e);
                                                      }
                                                  },
                                                  new ExecutionDataflowBlockOptions {
                                                                                        MaxDegreeOfParallelism = numberOfConcurrentWriters,
                                                                                        BoundedCapacity = numberOfConcurrentWriters
                                                  });
            
            while (true){
                String streamName = $"SalesTransactionAggregate-{Guid.NewGuid().ToString("n")}";
                await actionBlock.SendAsync(streamName);
            }

            Console.ReadKey();
        }

        static List<EventData> GetEvents(Int32 count){
            List<EventData> list = new();

            var payload = new {
                                  Id = Guid.NewGuid()
                              };

            String json = Newtonsoft.Json.JsonConvert.SerializeObject(payload);
            ReadOnlyMemory<Byte> rom = new(Encoding.Default.GetBytes(json));

            for (Int32 i = 0; i < count; i++){
                list.Add(new(Uuid.FromGuid(Guid.NewGuid()), "someType", rom));
            }

            return list;
        }

        static async Task CatchupTest(EventStoreClient client,String stream, StreamState state) {
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
            //Random random = new(state.GetHashCode());
            //Int32 val = random.Next(0, 10000);
            //Boolean timeout = val switch {
            //    0 => true,
            //    _ => false
            //};

            //if (timeout){
            //    Console.WriteLine($"Timeout occurred for Id [{state.Id}]");

            //    state = state with
            //            {
            //                NumberOfTimeouts = state.NumberOfTimeouts + 1
            //            };
            //}

            state = state with
                    {
                        DateTimeOfLastEvent = DateTime.Now,
                        EventsProcessed = state.EventsProcessed + 1
                    };

            streamStats[id] = state; //overwrite

            //if (timeout){
                //This event should timeout.
                //await Task.Delay(TimeSpan.FromSeconds(30), arg3);
            //}

            if ((totalEventsProcessed % 50000) == 0) {
                //Dump out report
                streamStats.ToList()
                           .ForEach(x => Console.WriteLine($"Stream Id [{x.Value.Id}] has processed [{x.Value.EventsProcessed}] events. Last processed [{x.Value.DateTimeOfLastEvent}]."));
            }
        }
    }
}
