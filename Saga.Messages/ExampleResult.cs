using System;

namespace Saga.Messages
{
    public class ExampleResult
    {
        public Guid RequestId { get; set; }
        public int Sum { get; set; }
    }
}