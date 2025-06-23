using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var userService = new UserService(new MessageBus("UserService"));
var emailService = new EmailService(new MessageBus("EmailService"));
var client = new MessageBus("Client");

await userService.InitialiseAsync();
await emailService.InitialiseAsync();
await client.InitailiseAsync();

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

public class MessageBus(
    string _serviceName, 
    string _connectionString = "amqp://guest:guest@localhost:5672" ) 
    : IAsyncDisposable
{
    private IConnection _connection;
    private IChannel _channel;
    private string _queueName;
    private readonly List<Type> _messageTypes = new();
    private readonly Dictionary<string, List<Func<object, Task>>> _handlers = new();
    
    public async Task InitailiseAsync()
    {
        var factory = new ConnectionFactory
        {
            Uri = new Uri(_connectionString),
            ClientProvidedName = _serviceName
        };

        _connection = await factory.CreateConnectionAsync();
        _channel = await _connection.CreateChannelAsync();
        _queueName = $"{_serviceName}.queue";
        
        await _channel.QueueDeclareAsync(_queueName, true, false, false);
        
        foreach (var typeName in _handlers.Keys)
        {
            var exchangeName = typeName;
            await _channel.ExchangeDeclareAsync(exchangeName, ExchangeType.Fanout, true);
            await _channel.QueueBindAsync(_queueName, exchangeName, "");
        }
        
        await SetupConsumerAsync();
    }

    public async Task SendAsync<TMessage>(TMessage message) where TMessage : IMessage
    {
        var exchangeName = typeof(TMessage).Name;
        
        await _channel.ExchangeDeclareAsync(exchangeName, ExchangeType.Fanout, true);
        var messageBody = JsonSerializer.Serialize(message);
        var body = Encoding.UTF8.GetBytes(messageBody);
        var props = new BasicProperties
        {
            Type = typeof(TMessage).Name,
        };

        await _channel.BasicPublishAsync(exchangeName, "", true, props, body);
    }
    
    public async Task SendCommandAsync<TCommand>(TCommand command) where TCommand : ICommand
        => await SendAsync(command);
    
    public async Task PublishEventAsync<TEvent>(TEvent @event) where TEvent : IEvent
        => await SendAsync(@event);

    public void Subscribe<TMessage>(Func<TMessage, Task> handler) where TMessage : IMessage
    {
        var messageType = typeof(TMessage);
        var messageTypeName = messageType.Name;
        
        if(!_messageTypes.Contains(messageType))
            _messageTypes.Add(messageType);
        
        if(!_handlers.ContainsKey(messageTypeName))
            _handlers.Add(messageTypeName, new List<Func<object, Task>>());
        
        _handlers[messageTypeName].Add(async (message) => await handler((TMessage)message));
    }
    
    public void SubscribeToCommand<TCommand>(Func<TCommand, Task> handler) where TCommand : ICommand
        => Subscribe(handler);
    
    public void SubscribeToEvent<TEvent>(Func<TEvent, Task> handler) where TEvent : IEvent
        => Subscribe(handler);
    
    private async Task SetupConsumerAsync()
    {
        var consumer = new AsyncEventingBasicConsumer(_channel);
        consumer.ReceivedAsync += async (sender, args) =>
        {
            try
            {
                var body = args.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                var messageTypeName = args.BasicProperties.Type;

                if (!string.IsNullOrEmpty(messageTypeName))
                {
                    var messageType = _messageTypes.SingleOrDefault(t => t.Name == messageTypeName);
                    
                    if (messageType != null && _handlers.TryGetValue(messageTypeName, out var handlerList))
                    {
                        var deserializedMessage = JsonSerializer.Deserialize(message, messageType);
                        if (deserializedMessage != null)
                        {
                            var tasks = handlerList.Select(handler => handler(deserializedMessage));
                            await Task.WhenAll(tasks);
                        }
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
    private readonly MessageBus _bus;

    public UserService(MessageBus bus)
    {
        _bus = bus;
        _bus.Subscribe<CreateUserCommand>(HandleCreateUser);
        _bus.Subscribe<OrderPlacedEvent>(HandleOrderPlaced);
    }

    public async Task InitialiseAsync()
    {
        await _bus.InitailiseAsync();
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

    public EmailService(MessageBus bus)
    {
        _bus = bus;
        _bus.Subscribe<CreateUserCommand>(HandleCreateUser);
        _bus.Subscribe<UserCreatedEvent>(SendWelcomeEmail);
    }

    public async Task InitialiseAsync()
    {
        await _bus.InitailiseAsync();
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