using RabbitMQ.Client;
using System;
using System.Text;

namespace publisher
{
	internal sealed class Program
	{
		static void Main(string[] args)
		{
			var publisher = new Publisher();
			publisher.Send("hello");

			Console.ReadKey();
		}
	}

	public sealed class Publisher
	{
		public const string DEFAULT_QUEUE_NAME = "defaultQueue";
		public const string DEFAULT_EXCHANGE_NAME = "defaultExchange";

		public void Send(string message)
		{
			var factory = new ConnectionFactory() { HostName = "localhost" };

			using (var connection = factory.CreateConnection())
			{
				using (var channel = connection.CreateModel())
				{
					var properties = channel.CreateBasicProperties();

					channel.ExchangeDeclare(Publisher.DEFAULT_EXCHANGE_NAME, ExchangeType.Fanout);
					channel.QueueBind(Publisher.DEFAULT_QUEUE_NAME, Publisher.DEFAULT_EXCHANGE_NAME, string.Empty);

					//create a queue when isnt set
					//channel.QueueDeclare(queue: Publisher.DEFAULT_QUEUE_NAME,
					//	//save queue when rabbit dies
					//		durable: true,
					//		exclusive: default,
					//		autoDelete: default,
					//		arguments: null);

					var body = Encoding.UTF8.GetBytes(message);

					channel.BasicPublish(
							exchange: Publisher.DEFAULT_EXCHANGE_NAME,
							routingKey: Publisher.DEFAULT_QUEUE_NAME,
							basicProperties: properties,
							body: body);

					Console.WriteLine("pulling message to queue");
				}
			}
		}
	}
}
