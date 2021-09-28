using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Nest;
using Online.Shipments.Kafka.Models.Types;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ShipmentsOnTheMapIndexingTool.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class ShipmentController : ControllerBase
    {
        readonly IElasticClient elasticClientDevIndex = CreateElasticClient("online-tracked-shipments");
        readonly IElasticClient elasticClientTestIndex = CreateElasticClient("online-tracked-shipments-test");
        readonly List<Location> locations = DataList.locations;

        private readonly ILogger<ShipmentController> _logger;

        public ShipmentController(ILogger<ShipmentController> logger)
        {
            _logger = logger;
        }

        [HttpGet]
        public async Task<IEnumerable<TestSearchShipments>> Get()
        {
            ISearchRequest selector(SearchDescriptor<TestSearchShipments> s) => s
                    .Size(locations.Count)
                    .Query(q => q.Terms(t => t
                        .Field(f => f.Status.Suffix("raw"))
                        .Terms(new List<string>() { "In Transit", "Delivered" })));

            var searchResponse = await elasticClientDevIndex.SearchAsync<TestSearchShipments>(selector);

            int index = 0;
            var shipmentsWithTracking = new List<TestSearchShipments>(locations.Count);
            
            foreach (var hit in searchResponse.Hits)
            {
                var shipment = hit.Source;
                var stops = shipment.Stops;
                var stopId = stops.LastOrDefault()?.StopId ?? "0";
                var partyCodes = stops.LastOrDefault()?.BillTosPartyCodes ?? new List<string>();

                shipment.TrackingInfo = new TrackingInfo
                {
                    PredictiveTracking = new PredictiveTracking
                    {
                        RiskInfo = new RiskInfo
                        {
                            Risk = shipment.Status.Equals("In Transit") && index % 10 == 0 ? true : false,
                            StopId = stopId,
                            BillTosPartyCodes = partyCodes
                        },
                        EtaInfo = new EtaInfo
                        {
                            EtaNextStopLocal = DateTimeOffset.UtcNow,
                            LastUpdatedDate = DateTimeOffset.UtcNow.AddDays(-1),
                            StopId = stopId,
                            BillTosPartyCodes = partyCodes
                        },
                    },
                    LocationTracking = new List<LocationTracking>
                    {
                        new LocationTracking
                        {
                           StopId = stopId,
                           EventDateTime = DateTimeOffset.UtcNow,
                           Location =  locations[index]
                        }
                    }
                };

                shipmentsWithTracking.Add(shipment);

                index++;
            }

            elasticClientTestIndex.BulkAll(shipmentsWithTracking, b => b
            .BufferToBulk((des, list)=> 
            {
                foreach (var shipment in list)
                {
                    des.Index<TestSearchShipments>(b2 => b2
                       .Document(shipment)
                       .Id(shipment.ShipmentNumber) // Override the ES document id
                    );
                }
            })).Wait(TimeSpan.FromSeconds(15), null);

            return new List<TestSearchShipments> { shipmentsWithTracking[0] };
        }

        private static IElasticClient CreateElasticClient(string indexName)
        {
            var connectionSettings = new ConnectionSettings(new Uri("https://d6a29425c41b489ebc37d34ab4d9a392.ece1-np.ddschr.local:9243"))
               .DefaultIndex(indexName)
               .EnableHttpCompression()
               .ThrowExceptions()
               .BasicAuthentication("online_shipments_producer", "jRwAqxw391nweDwjaq");

            var elasticClient = new ElasticClient(connectionSettings);
            return elasticClient;
        }

        public class TestSearchShipments : SearchShipment
        {
            public TrackingInfo TrackingInfo { get; set; }
        }

        public class TrackingInfo
        {
            public PredictiveTracking PredictiveTracking { get; set; }
            public List<LocationTracking> LocationTracking { get; set; }

        }

        public class PredictiveTracking
        {
            public RiskInfo RiskInfo { get; set; }
            public EtaInfo EtaInfo { get; set; }

        }

        public class LocationTracking
        {
            public int ItemContainerId { get; set; }
            public DateTimeOffset EventDateTime { get; set; }
            public string StopId { get; set; }
            public string TrackingId { get; set; }
            public Location Location { get; set; }
        }

        public class RiskInfo
        {
            public bool? Risk { get; set; }
            public string StopId { get; set; }
            public List<string> BillTosPartyCodes { get; set; } = new List<string>();
            public List<string> SupplierPartyCodes { get; set; } = new List<string>();
        }

        public class EtaInfo
        {
            public DateTimeOffset? EtaNextStopLocal { get; set; }
            public DateTimeOffset? LastUpdatedDate { get; set; }
            public List<string> BillTosPartyCodes { get; set; } = new List<string>();
            public List<string> SupplierPartyCodes { get; set; } = new List<string>();
            public string StopId { get; set; }
        }
    }
}

