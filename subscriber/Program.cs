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
			var sub = new Subscriber();
			sub.PopMessage(10000);
		}
	}

	internal sealed class Subscriber
	{
		public void PopMessage(int sleepTime)
		{
			var factory = new ConnectionFactory() { HostName = "localhost" };

			using (var connection = factory.CreateConnection())
			{
				using (var channel = connection.CreateModel())
				{
					channel.QueueDeclare(queue: publisher.Publisher.DEFAULT_QUEUE_NAME,
							durable: default,
							exclusive: default,
							autoDelete: default,
							arguments: null);
					channel.BasicQos(0, 1, default);

					var listener = new EventingBasicConsumer(channel);
					listener.Received += (model, eventInfo) =>
					{
						if (eventInfo?.Body.Length == default)
						{
							throw new InvalidOperationException("message has no body");
						}

						var body = Encoding.UTF8.GetString(eventInfo.Body.ToArray());

						Thread.Sleep(sleepTime);

						Console.WriteLine($"publisher says: {body}");
						channel.BasicAck(eventInfo.DeliveryTag, default);
					};

					channel.BasicConsume(
							queue: publisher.Publisher.DEFAULT_QUEUE_NAME,
							//autoAck: true,
							consumer: listener
							);

					Console.ReadKey();
				}
			}
		}
	}
}
