using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var userService = await UserService.CreateAsync();
var emailService = await EmailService.CreateAsync();

var client = await MessageBus.CreateAsync("Client");

await client.SendAsync(new CreateUserCommand("John Doe", "john.doe@example.com"));

Console.ReadKey();

await client.DisposeAsync();
await userService.DisposeAsync();
await emailService.DisposeAsync();
    
public interface IMessage
{
}

public interface ICommand : IMessage
{
}

public interface IEvent : IMessage
{
}

public class MessageBus : IAsyncDisposable
{
    private IConnection _connection;
    private IChannel _channel;
    private string _queueName;
    private readonly Dictionary<Type, List<Func<object, Task>>> _handlers = new();

    private MessageBus()
    {
    }

    public static async Task<MessageBus> CreateAsync(
        string serviceName,
        string connectionString = "amqp://guest:guest@localhost:5672")
    {
        var bus = new MessageBus();
        await bus.InitailiseAsync(serviceName, connectionString);
        return bus;
    }
    
    private async Task InitailiseAsync(string serviceName, string connectionString)
    {
        var factory = new ConnectionFactory
        {
            Uri = new Uri(connectionString),
            ClientProvidedName = serviceName
        };

        _connection = await factory.CreateConnectionAsync();
        _channel = await _connection.CreateChannelAsync();
        _queueName = $"{serviceName}.queue";
        
        await _channel.QueueDeclareAsync(_queueName, true, false, false);

        await SetupConsumerAsync();
    }

    public async Task SendAsync<T>(T message) where T : IMessage
    {
        var exchangeName = typeof(T).Name;
        
        await _channel.ExchangeDeclareAsync(exchangeName, ExchangeType.Fanout, true);
        var messageBody = JsonSerializer.Serialize(message);
        var body = Encoding.UTF8.GetBytes(messageBody);
        var props = new BasicProperties
        {
            Type = typeof(T).Name,
        };

        await _channel.BasicPublishAsync(exchangeName, "", true, props, body);
    }
    
    public async Task SendCommandAsync<T>(T command) where T : ICommand
        => await SendAsync(command);
    
    public async Task PublishEventAsync<T>(T @event) where T : IEvent
        => await SendAsync(@event);

    public async Task SubscribeAsync<T>(Func<T, Task> handler) where T : IMessage
    {
        var exchangeName = typeof(T).Name;
        
        await _channel.ExchangeDeclareAsync(exchangeName, ExchangeType.Fanout, true);
        await _channel.QueueBindAsync(_queueName, exchangeName, "");
        
        if(!_handlers.ContainsKey(typeof(T)))
            _handlers.Add(typeof(T), new List<Func<object, Task>>());
        
        _handlers[typeof(T)].Add(async (message) => await handler((T)message));
    }
    
    public async Task SubscribeToCommandAsync<T>(Func<T, Task> handler) where T : ICommand
        => await SubscribeAsync(handler);
    
    public async Task SubscribeToEventAsync<T>(Func<T, Task> handler) where T : IEvent
        => await SubscribeAsync(handler);
    
    private async Task SetupConsumerAsync()
    {
        var consumer = new AsyncEventingBasicConsumer(_channel);
        consumer.ReceivedAsync += async (sender, args) =>
        {
            try
            {
                var body = args.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                var messageType = args.BasicProperties.Type;

                if (!string.IsNullOrEmpty(messageType))
                {
                    var type = AppDomain.CurrentDomain.GetAssemblies()
                        .SelectMany(a => a.GetTypes())
                        .FirstOrDefault(t => t.Name == messageType);

                    if (type != null && _handlers.ContainsKey(type))
                    {
                        var deserializedMessage = JsonSerializer.Deserialize(message, type);
                        var tasks = _handlers[type].Select(handler => handler(deserializedMessage));
                        await Task.WhenAll(tasks);
                    }
                }

                await _channel.BasicAckAsync(args.DeliveryTag, false);
            }
            catch (Exception ex)
            {
                await _channel.BasicNackAsync(args.DeliveryTag, false, false);
            }
        };
        
        await _channel.BasicConsumeAsync(_queueName, false, consumer);
    }

    public async ValueTask DisposeAsync()
    {
        if (_channel != null)
        {
            await _channel.CloseAsync();
            await _channel.DisposeAsync();
        }
        
        if (_connection != null)
        {
            await _connection.CloseAsync();
            await _connection.DisposeAsync();
        }
    }
}

public record CreateUserCommand(string Name, string Email) : ICommand;

public record UserCreatedEvent(Guid UserId, string Name, string Email) : IEvent;
public record OrderPlacedEvent(Guid OrderId, Guid UserId, decimal Amount) : IEvent;

public class UserService : IAsyncDisposable
{
    private MessageBus _bus;

    private UserService()
    {
    }

    public static async Task<UserService> CreateAsync()
    {
        var service = new UserService();
        await service.InitialiseAsync();
        return service;
    }

    private async Task InitialiseAsync()
    {
        _bus = await MessageBus.CreateAsync("UserService");

        await _bus.SubscribeAsync<CreateUserCommand>(HandleCreateUser);
        await _bus.SubscribeAsync<OrderPlacedEvent>(HandleOrderPlaced);
    }

    private async Task HandleCreateUser(CreateUserCommand command)
    {
        var userId = Guid.NewGuid();
        Console.WriteLine($"UserService: Creating user {command.Name}");
        
        await _bus.SendAsync(new UserCreatedEvent(userId, command.Name, command.Email));
    }

    private async Task HandleOrderPlaced(OrderPlacedEvent @event)
    {
        Console.WriteLine($"UserService: Order placed {@event.OrderId}");
        await Task.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        if(_bus != null)
            await _bus.DisposeAsync();
    }
}

public class EmailService : IAsyncDisposable
{
    private MessageBus _bus;

    private EmailService()
    {
    }

    public static async Task<EmailService> CreateAsync()
    {
        var service = new EmailService();
        await service.InitialiseAsync();
        return service;
    }

    private async Task InitialiseAsync()
    {
        _bus = await MessageBus.CreateAsync("EmailService");

        await _bus.SubscribeAsync<CreateUserCommand>(HandleCreateUser);
        await _bus.SubscribeAsync<UserCreatedEvent>(SendWelcomeEmail);
    }

    private async Task HandleCreateUser(CreateUserCommand command)
    {
        Console.WriteLine($"EmailService: Preparing email setup for {command.Name}");
        
        await Task.CompletedTask;
    }

    private async Task SendWelcomeEmail(UserCreatedEvent @event)
    {
        Console.WriteLine($"EmailService: Sending welcome email to {@event.Email}");
        await Task.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        if(_bus != null)
            await _bus.DisposeAsync();
    }
}