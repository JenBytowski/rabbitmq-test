using RabbitMQ.Client;
using System;
using System.Text;

namespace publisher
{
	internal sealed class Program
	{
		static void Main(string[] args)
		{
			//var publisher = new StringMessagePublisher(queue: "logQueue", exchanger: "log", routingKey: "log");
			//publisher.Send("hello");

			var topicPublisher = new StringMessageTopicPublisher(exchanger: "log", routingKey: args[0]);
			topicPublisher.Send("topic sender says hello");

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

		protected Publisher(string queue, string exchanger, string routingKey)
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

					channel.ExchangeDeclare(this.exchanger, ExchangeType.Direct);
					//channel.QueueBind(this.queue, this.exchanger, this.routingKey);

					var body = Encoding.UTF8.GetBytes(message);

					channel.BasicPublish(
							exchange: this.exchanger,
							//possible to change this option
							routingKey: this.routingKey,
							basicProperties: properties,
							body: body);

					Console.WriteLine("pulling string message to queue");
				}
			}
		}
	}

	internal sealed class StringMessageTopicPublisher : Publisher<string>
	{
		public StringMessageTopicPublisher(string exchanger, string routingKey, string queue = default)
			: base(queue, exchanger, routingKey)
		{ }

		public override void Send(string message)
		{
			var factory = new ConnectionFactory() { HostName="localhost" };

			using (var connection = factory.CreateConnection())
			{
				using (var channel = connection.CreateModel())
				{
					channel.ExchangeDeclare(this.exchanger, ExchangeType.Topic);

					var body = Encoding.UTF8.GetBytes(message);

					channel.BasicPublish(this.exchanger, this.routingKey, null, body);
					Console.WriteLine($"{nameof(StringMessageTopicPublisher)} send message to {this.exchanger}");
				}
			}
		}
	}
}
