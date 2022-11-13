using Confluent.Kafka;
using System.Text;

namespace DesenvolvedorIO.Tips
{
    public static class Produtor
    {
        public static async Task EnviarMessagem(string topico, int i)
        {
            var configuracao = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",

                // Habilitar idempotência, degrada um pouco a performance da aplicacao porem nao tera mensagem duplicadas
                EnableIdempotence = true, // Evita que mensagem seja duplicada no broker
                Acks = Acks.All,
                MaxInFlight = 1,
                MessageSendMaxRetries = 2,

                TransactionalId = Guid.NewGuid().ToString()
            };

            var key = Guid.NewGuid().ToString();
            var mensagem = $"Mensagem ( {i} ) KEY: {key}";

            //var payload = System.Text.Json.JsonSerializer.Serialize(mensagem); //Serilizar em Json

            Console.WriteLine(">> Enviada:\t " + mensagem);

            try
            {
                // Instância
                using var producer = new ProducerBuilder<string, string>(configuracao).Build();

                //Iniciar uma transação
                producer.InitTransactions(TimeSpan.FromSeconds(5));
                producer.BeginTransaction();

                var headers = new Headers();
                headers.Add("application", Encoding.UTF8.GetBytes("payment"));
                headers.Add("transactionId", Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()));

                var topicParticion = new TopicPartition(topico, 2);

                var result = await producer.ProduceAsync(topicParticion, new Message<string, string>
                {
                    Key = key,
                    Value = mensagem,
                    Headers = headers
                });
                // Enviar Mensagem 2
                // Enviar Mensagem 3
                // Atualizar status no banco de dados

                // Confirma a transação
                producer.CommitTransaction();

                // Em caso de erro pode abortar a transação
                //producer.AbortTransaction();

                await Task.CompletedTask;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.ToString());
            }
        }
    }
}