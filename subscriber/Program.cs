using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Threading;

namespace subscriber
{
	internal sealed class Program
	{
		static void Main(string[] args)
		{
			//var sub = new BaseSubscriber(queue: "stringQueue", exchanger: "stringMessageExchanger");
			//sub.GetMessage();

			var loggerSub = new LogSubscriber(queue: "logQueue", exchanger: "log", routingKey: args[0]);
			loggerSub.GetMessage();
		}
	}

	public abstract class Subscriber
	{
		public const string DEFAULT_QUEUE_NAME = "defaultQueue";
		public const string DEFAULT_EXCHANGE_NAME = "defaultExchange";

		protected readonly string queue;
		protected readonly string exchanger;
		protected readonly string routingKey;

		protected Subscriber(string queue, string exchanger, string routingKey)
		{
			this.queue = queue;
			this.exchanger = exchanger;
			this.routingKey = routingKey;
		}

		public abstract void GetMessage();
	}

	internal sealed class BaseSubscriber : Subscriber
	{
		public BaseSubscriber(string queue = default, string exchanger = default, string routingKey = default)
			: base(queue ?? Subscriber.DEFAULT_QUEUE_NAME, exchanger ?? Subscriber.DEFAULT_EXCHANGE_NAME, routingKey ?? string.Empty)
		{
		}

		public override void GetMessage()
		{
			var factory = new ConnectionFactory() { HostName = "localhost" };

			using (var connection = factory.CreateConnection())
			{
				using (var channel = connection.CreateModel())
				{
					// create a queue when isnt set
					channel.QueueDeclare(
							queue: this.queue,
							//save queue when rabbit dies
							durable: default,
							exclusive: default,
							autoDelete: default,
							arguments: null);
					channel.BasicQos(0, 1, default);
					channel.QueueBind(this.queue, this.exchanger, this.routingKey);

					var listener = new EventingBasicConsumer(channel);
					listener.Received += (model, eventInfo) =>
					{
						if (eventInfo?.Body.Length == default)
						{
							throw new InvalidOperationException("message has no body");
						}

						var body = Encoding.UTF8.GetString(eventInfo.Body.ToArray());

						Thread.Sleep(7000);

						Console.WriteLine($"publisher says: {body}");
						channel.BasicAck(eventInfo.DeliveryTag, default);
					};

					channel.BasicConsume(
							queue: this.queue,
							consumer: listener
							);

					Console.ReadKey();
				}
			}
		}
	}

	internal sealed class LogSubscriber : Subscriber
	{
		public LogSubscriber(string queue = default, string exchanger = default, string routingKey = default)
			: base(queue ?? "logQueue", exchanger ?? "logMessageExchanger", routingKey ?? string.Empty)
		{
		}

		public override void GetMessage()
		{
			var factory = new ConnectionFactory();

			using (var connection = factory.CreateConnection())
			{
				using (var channel = connection.CreateModel())
				{
					channel.ExchangeDeclare(this.exchanger, ExchangeType.Topic);
					channel.QueueDeclare(
						queue: this.queue,
						durable: default,
						exclusive: default,
						autoDelete: default);
					channel.QueueBind(this.queue, this.exchanger, this.routingKey);

					var listener = new EventingBasicConsumer(channel);
					listener.Received += (model, args) =>
					{
						var body = args.Body.ToArray();
						var message = Encoding.UTF8.GetString(body);

						Console.WriteLine($"properties of connection ex: {args.Exchange}, routing key: {args.RoutingKey}");
						Console.WriteLine(message);

						channel.BasicAck(args.DeliveryTag, default);
					};

					channel.BasicConsume(this.queue, default, listener);
					Console.ReadKey();
				}
			}
		}
	}
}
