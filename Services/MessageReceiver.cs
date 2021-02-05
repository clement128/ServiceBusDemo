using Azure.Messaging.ServiceBus;
using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace ServiceBugDemo.Services
{
    public class MessageReceiver : BackgroundService
    {
        const string QUEUE_NAME = "Test";
        private readonly ServiceBusClient serviceBusClient;
        private readonly ServiceBusProcessor processor;

        public MessageReceiver(IOptions<AppOptions> appOptions)
        {
            serviceBusClient = new ServiceBusClient(appOptions.Value.Connectionstring);
            processor = serviceBusClient.CreateProcessor(QUEUE_NAME, new ServiceBusProcessorOptions
            {
                MaxConcurrentCalls = 1
            });
            processor.ProcessMessageAsync += MessageHandler;
            processor.ProcessErrorAsync += ErrorHandler;
        }

        private Task ErrorHandler(ProcessErrorEventArgs args)
        {
            return Console.Error.WriteAsync(args.Exception.ToString());
        }

        private async Task MessageHandler(ProcessMessageEventArgs args)
        {
            string body = args.Message.Body.ToString();
            Console.WriteLine(body);

            // we can evaluate application logic and use that to determine how to settle the message.
            await args.CompleteMessageAsync(args.Message);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await processor.StartProcessingAsync(stoppingToken);
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            await processor.DisposeAsync();
            await serviceBusClient.DisposeAsync();
            await base.StopAsync(cancellationToken);
        }
    }
}
