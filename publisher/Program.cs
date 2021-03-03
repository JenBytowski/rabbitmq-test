using RabbitMQ.Client;
using System;
using System.Text;

namespace publisher
{
	internal sealed class Program
	{
		static void Main(string[] args)
		{
			var publisher = new StringMessagePublisher();
			publisher.Send("hello");

			Console.ReadKey();
		}
	}

	public abstract class Publisher<T> where T : class
	{
		public const string DEFAULT_QUEUE_NAME = "defaultQueue";
		public const string DEFAULT_EXCHANGE_NAME = "defaultExchange";

		protected readonly string queue;
		protected readonly string exchanger;
		protected readonly string routingKey;

		protected Publisher(string queue = default, string exchanger = default, string routingKey = default)
		{
			this.queue = queue;
			this.exchanger = exchanger;
			this.routingKey = routingKey;
		}

		public abstract void Send(T message);
	}

	internal class StringMessagePublisher : Publisher<string>
	{
		public StringMessagePublisher(string queue = default, string exchanger = default, string routingKey = default)
			: base(queue ?? "stringQueue", exchanger ?? "stringMessageExchanger", routingKey ?? string.Empty)
		{
		}

		public override void Send(string message)
		{
			var factory = new ConnectionFactory() { HostName = "localhost" };

			using (var connection = factory.CreateConnection())
			{
				using (var channel = connection.CreateModel())
				{
					var properties = channel.CreateBasicProperties();

					//create a queue when isnt set
					channel.QueueDeclare(
							queue: this.queue,
							//save queue when rabbit dies
							durable: default,
							exclusive: default,
							autoDelete: default,
							arguments: null);

					channel.ExchangeDeclare(this.exchanger, ExchangeType.Fanout);
					channel.QueueBind(this.queue, this.exchanger, this.routingKey);

					var body = Encoding.UTF8.GetBytes(message);

					channel.BasicPublish(
							exchange: this.exchanger,
							routingKey: this.queue,
							basicProperties: properties,
							body: body);

					Console.WriteLine("pulling string message to queue");
				}
			}
		}
	}
}
