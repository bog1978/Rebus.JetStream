using System;

namespace Saga.Messages
{
    public class ExampleRequest
    {
        public Guid RequestId { get; set; }
        public int A { get; set; }
        public int B { get; set; }
    }
}
