using Confluent.Kafka;
using System.Text;

namespace DesenvolvedorIO.Tips
{
    public static class Consumidor
    {
        public static void Iniciar(string topico, string consumerId, AutoOffsetReset autoOffsetReset)
        {
            var clientId = Guid.NewGuid().ToString().Substring(0, 5);

            var conf = new ConsumerConfig
            {
                ClientId = clientId,
                GroupId = consumerId,
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = autoOffsetReset,
                EnablePartitionEof = true,
                EnableAutoCommit = false,
                EnableAutoOffsetStore = false,

                //Consome apenas as mensagens commitadas pelo produtor na transacao
                //IsolationLevel = IsolationLevel.ReadCommitted,
            };

            using var consumer = new ConsumerBuilder<string, string>(conf).Build();

            consumer.Subscribe(topico);

            while (true)
            {
                var topicParticion = new TopicPartition(topico, 2);
                consumer.Assign(topicParticion);

                var result = consumer.Consume();

                if (result.IsPartitionEOF) // Permance o loop até que mensagem seja totalmente lida
                {
                    continue; // pula para proxima interacao do loop
                }

                //var message = System.Text.Json.JsonSerializer.Deserialize<string>(result.Message.Value); // Deserilizar o json

                //Utilizado para realizar tracking dos eventos
                var headers = result
                    .Message
                    .Headers
                    .ToDictionary(p => p.Key, p => Encoding.UTF8.GetString(p.GetValueBytes()));

                var application = headers["application"];
                var transactionId = headers["transactionId"];

                //var messsage = "<< Recebida: \t" + result.Message.Value;
                var messsage = "<< Recebida: \t" + result.Message.Value;
                Console.WriteLine(messsage);

                consumer.Commit(result);
                consumer.StoreOffset(result.TopicPartitionOffset);
            }
        }
    }
}