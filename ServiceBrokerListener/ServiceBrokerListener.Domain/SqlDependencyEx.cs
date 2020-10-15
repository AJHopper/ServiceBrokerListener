using System.IO;
using System.Xml;
using System.Linq;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.Threading;
using System.Xml.Linq;
using System.Xml.Serialization;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace ServiceBrokerListener.Domain
{
    [Flags]
    public enum NotificationTypes
    {
        None = 0,
        Insert = 1 << 1,
        Update = 1 << 2,
        Delete = 1 << 3
    }

    #region MessageAttributes
    /// <summary>Causes Batch Listener To Use This Field To Group</summary>
    public class GroupBy : Attribute
    {
        public virtual bool GroupByVariable => true;
    }

    /// <summary>Causes Batch Listener To Use This Field To Delete - Should Be Id Field</summary>
    public class DeleteOn : Attribute
    {
        public virtual bool DeleteOnVariable => true;
    }

    /// <summary>Causes Batch Listener To Stuff Group Result Of This Field To Single Field</summary>
    public class SqlStuff : Attribute
    {
        public virtual bool SqlStuffVariable => true;
    }

    /// <summary>Specifies The Name Of The Table This Field Belongs To</summary>
    public class Table : Attribute
    {
        private readonly string name;

        public virtual string Name => name;

        public Table(string TableName)
        {
            name = TableName;
        }

    }

    /// <summary>Marks This Field As A System Field To Exclude Wrapping It In "[]"</summary>
    public class IsSystem : Attribute
    {
        public virtual bool IsSystemVariable => true;
    }

    /// <summary>Specifies The Name Of The Column This Field Uses In The Database</summary>
    public class Column : Attribute
    {
        private readonly string name;

        public virtual string Name => name;

        public Column(string ColumnName)
        {
            name = ColumnName;
        }

    }

    /// <summary>Marks The Field To Be Excluded From The Selected Fields</summary>
    public class ExcludeFromSelect : Attribute
    {
        public virtual bool Exclude => true;
    }

    #endregion


    /// <summary>Abstract Base Class To Storage Of Listeners Into Container Without Specific Typing</summary>
    public abstract class ServiceBrokerListener
    {
        public abstract (int identity, string tableName, bool active, long processedCount) Status();
        public abstract int GetId();

        public abstract bool Install();
        public abstract bool Uninstall();
        public abstract bool Start();
        public abstract bool Stop();
    }

    public abstract class ServiceBrokerListenerBase<T> : ServiceBrokerListener, IDisposable where T : class
    {
        #region Scripts

        #region Procedures

        protected const string SQL_PERMISSIONS_INFO = @"
                    DECLARE @msg VARCHAR(MAX)
                    DECLARE @crlf CHAR(1)
                    SET @crlf = CHAR(10)
                    SET @msg = 'Current user must have following permissions: '
                    SET @msg = @msg + '[CREATE PROCEDURE, CREATE SERVICE, CREATE QUEUE, SUBSCRIBE QUERY NOTIFICATIONS, CONTROL, REFERENCES] '
                    SET @msg = @msg + 'that are required to start query notifications. '
                    SET @msg = @msg + 'Grant described permissions with following script: ' + @crlf
                    SET @msg = @msg + 'GRANT CREATE PROCEDURE TO [<username>];' + @crlf
                    SET @msg = @msg + 'GRANT CREATE SERVICE TO [<username>];' + @crlf
                    SET @msg = @msg + 'GRANT CREATE QUEUE  TO [<username>];' + @crlf
                    SET @msg = @msg + 'GRANT REFERENCES ON CONTRACT::[DEFAULT] TO [<username>];' + @crlf
                    SET @msg = @msg + 'GRANT SUBSCRIBE QUERY NOTIFICATIONS TO [<username>];' + @crlf
                    SET @msg = @msg + 'GRANT CONTROL ON SCHEMA::[<schemaname>] TO [<username>];'
                    
                    PRINT @msg
                ";

        #endregion

        #region ServiceBroker Notificitation

        /*
         T-SQL script-template which prepares database for ServiceBroker notification.
         {0} - database name;
         {1} - conversation queue name.
         {2} - conversation service name.
         {3} - schema name.        
        */
        protected const string SQL_FORMAT_INSTALL_SEVICE_BROKER_NOTIFICATION = @"
                -- Setup Service Broker
                IF EXISTS (SELECT * FROM sys.databases 
                                    WHERE name = '{0}' AND (is_broker_enabled = 0 OR is_trustworthy_on = 0)) 
                BEGIN
                --    ALTER DATABASE [{0}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
                --    ALTER DATABASE [{0}] SET ENABLE_BROKER; 
                --    ALTER DATABASE [{0}] SET MULTI_USER WITH ROLLBACK IMMEDIATE
                --    -- FOR SQL Express
                --    ALTER AUTHORIZATION ON DATABASE::[{0}] TO [sa]
                    ALTER DATABASE [{0}] SET TRUSTWORTHY ON;
                END
                -- Create a queue which will hold the tracked information 
                IF NOT EXISTS (SELECT * FROM sys.service_queues WHERE name = '{1}')
	                CREATE QUEUE {3}.[{1}]
                -- Create a service on which tracked information will be sent 
                IF NOT EXISTS(SELECT * FROM sys.services WHERE name = '{2}')
	                CREATE SERVICE [{2}] ON QUEUE {3}.[{1}] ([DEFAULT]) 
            ";

        /*
         T-SQL script-template which removes database notification.
         {0} - conversation queue name.
         {1} - conversation service name.
         {2} - schema name.
        */
        protected const string SQL_FORMAT_UNINSTALL_SERVICE_BROKER_NOTIFICATION = @"
                DECLARE @serviceId INT
                SELECT @serviceId = service_id FROM sys.services 
                WHERE sys.services.name = '{1}'
                DECLARE @ConvHandle uniqueidentifier
                DECLARE Conv CURSOR FOR
                SELECT CEP.conversation_handle FROM sys.conversation_endpoints CEP
                WHERE CEP.service_id = @serviceId AND ([state] != 'CD' OR [lifetime] > GETDATE() + 1)
                OPEN Conv;
                FETCH NEXT FROM Conv INTO @ConvHandle;
                WHILE (@@FETCH_STATUS = 0) BEGIN
    	            END CONVERSATION @ConvHandle WITH CLEANUP;
                    FETCH NEXT FROM Conv INTO @ConvHandle;
                END
                CLOSE Conv;
                DEALLOCATE Conv;
                -- Droping service and queue.
                DROP SERVICE [{1}];
                IF OBJECT_ID ('{2}.{0}', 'SQ') IS NOT NULL
	                DROP QUEUE {2}.[{0}];
            ";

        #endregion

        #region Miscellaneous

        /*
         T-SQL script-template which helps to receive changed data in monitorable table.
         {0} - database name.
         {1} - conversation queue name.
         {2} - timeout.
         {3} - schema name.
        */
        protected const string SQL_FORMAT_RECEIVE_EVENT = @"
                DECLARE @ConvHandle UNIQUEIDENTIFIER
                DECLARE @message VARBINARY(MAX)
                USE [{0}]
                WAITFOR (RECEIVE TOP(1) @ConvHandle=Conversation_Handle
                            , @message=message_body FROM {3}.[{1}]), TIMEOUT {2};
	            BEGIN TRY END CONVERSATION @ConvHandle; END TRY BEGIN CATCH END CATCH
                SELECT CAST(@message AS NVARCHAR(MAX)) 
            ";

        /*
         T-SQL script-template which executes stored procedure.
         {0} - database name.
         {1} - procedure name.
         {2} - schema name.
        */
        protected const string SQL_FORMAT_EXECUTE_PROCEDURE = @"
                USE [{0}]
                IF OBJECT_ID ('{2}.{1}', 'P') IS NOT NULL
                    EXEC {2}.{1}
            ";

        /* <summary>
         T-SQL script-template which returns all dependency identities in the database.
         {0} - database name.
        */
        protected const string SQL_FORMAT_GET_DEPENDENCY_IDENTITIES = @"
                USE [{0}]
                
                SELECT REPLACE(name , 'ListenerService_' , '') 
                FROM sys.services 
                WHERE [name] like 'ListenerService_%';
            ";

        #endregion

        #region Forced cleaning of database

        /*
         T-SQL script-template which cleans database from notifications.
         {0} - database name.
        */
        protected const string SQL_FORMAT_FORCED_DATABASE_CLEANING = @"
                USE [{0}]
                DECLARE @db_name VARCHAR(MAX)
                SET @db_name = '{0}' -- provide your own db name                
                DECLARE @proc_name VARCHAR(MAX)
                DECLARE procedures CURSOR
                FOR
                SELECT   sys.schemas.name + '.' + sys.objects.name
                FROM    sys.objects 
                INNER JOIN sys.schemas ON sys.objects.schema_id = sys.schemas.schema_id
                WHERE sys.objects.[type] = 'P' AND sys.objects.[name] like 'TradeCoordinator_UninstallListenerNotification_%'
                OPEN procedures;
                FETCH NEXT FROM procedures INTO @proc_name
                WHILE (@@FETCH_STATUS = 0)
                BEGIN
                EXEC ('USE [' + @db_name + '] EXEC ' + @proc_name + ' IF (OBJECT_ID (''' 
                                + @proc_name + ''', ''P'') IS NOT NULL) DROP PROCEDURE '
                                + @proc_name)
                FETCH NEXT FROM procedures INTO @proc_name
                END
                CLOSE procedures;
                DEALLOCATE procedures;
                DECLARE procedures CURSOR
                FOR
                SELECT   sys.schemas.name + '.' + sys.objects.name
                FROM    sys.objects 
                INNER JOIN sys.schemas ON sys.objects.schema_id = sys.schemas.schema_id
                WHERE sys.objects.[type] = 'P' AND sys.objects.[name] like 'TradeCoordinator_InstallListenerNotification_%'
                OPEN procedures;
                FETCH NEXT FROM procedures INTO @proc_name
                WHILE (@@FETCH_STATUS = 0)
                BEGIN
                EXEC ('USE [' + @db_name + '] DROP PROCEDURE '
                                + @proc_name)
                FETCH NEXT FROM procedures INTO @proc_name
                END
                CLOSE procedures;
                DEALLOCATE procedures;
            ";

        #endregion

        #endregion

        public long MessagesProcessed { get; protected set; }

        protected static ILogger<ServiceBrokerListener> _logger;

        public string ConnectionString { get; protected set; }

        public string DatabaseName { get; protected set; }

        public string TableName { get; protected set; }

        public string SchemaName { get; protected set; }

        public NotificationTypes NotificaionTypes { get; protected set; }

        public bool DetailsIncluded { get; protected set; }

        protected string SelectList = "";

        public int Identity
        {
            get;
            protected set;
        }

        public bool Active { get; protected set; }

        public override int GetId() => Identity;

        public override (int identity, string tableName, bool active, long processedCount) Status() => (Identity, TableName, Active, MessagesProcessed);

        public class TableChangedEventArgs : EventArgs
        {
            private readonly string notificationMessage;

            private const string INSERTED_TAG = "inserted";

            private const string DELETED_TAG = "deleted";

            public TableChangedEventArgs(string notificationMessage)
            {
                this.notificationMessage = notificationMessage;
                SetData();
            }


            private void SetData()
            {
                if (string.IsNullOrWhiteSpace(notificationMessage)) HeldData = (null, null);

                T Inserted = null;
                T Deleted = null;
                var doc = ReadXDocumentWithInvalidCharacters(notificationMessage);
                XmlSerializer deserializer = new XmlSerializer(typeof(T));

                try
                {
                    if (doc.Element(INSERTED_TAG) != null)
                    {
                        var InsertedXML = "<" + typeof(T).Name + ">\r\n" + string.Concat(doc.Element(INSERTED_TAG).Elements().First().Elements().Select(x => x.ToString() + "\r\n")) + "</" + typeof(T).Name + ">";
                        Inserted = (T)Convert.ChangeType(deserializer.Deserialize(new MemoryStream(Encoding.UTF8.GetBytes(InsertedXML))), typeof(T));
                    }
                    if (doc.Element(DELETED_TAG) != null)
                    {
                        var DeletedXML = "<" + typeof(T).Name + ">\r\n" + string.Concat(((XElement)doc.LastNode).Elements().First().Elements().Select(x => x.ToString() + "\r\n")) + "</" + typeof(T).Name + ">";
                        Deleted = (T)Convert.ChangeType(deserializer.Deserialize(new MemoryStream(Encoding.UTF8.GetBytes(DeletedXML))), typeof(T));
                    }

                    if (Inserted == null && Deleted == null)
                    {
                        _logger.LogWarning($"Both Inserted and Deleted are null after deserialisation \n Doc={doc}");
                    }
                }
                catch(Exception e)
                {
                    _logger.LogError($"Failed to deserialize {typeof(T)} \n Doc={doc} \n\t {e}");
                }


                HeldData = (Inserted, Deleted, notificationMessage);
            }

            //Storing the Inserted, Deleted and Raw Message - raw message is for debugging issues and should otherwise not be needed.
            private (T Inserted, T Deleted, string rawMessage) HeldData;
            
            public (T Inserted, T Deleted, string rawMessage) Data => HeldData;

            public NotificationTypes NotificationType => Data.Inserted != null ? Data.Deleted != null ? NotificationTypes.Update : NotificationTypes.Insert :
                        Data.Deleted != null ? NotificationTypes.Delete : NotificationTypes.None;

            /*             
                Returns the result XElement from the given xml string
            */
            private static XElement ReadXDocumentWithInvalidCharacters(string xml)
            {
                XDocument xDocument = null;

                XmlReaderSettings xmlReaderSettings = new XmlReaderSettings { CheckCharacters = false };

                using (var stream = new StringReader(xml))
                using (XmlReader xmlReader = XmlReader.Create(stream, xmlReaderSettings))
                {
                    // Load our XDocument
                    xmlReader.MoveToContent();
                    xDocument = XDocument.Load(xmlReader);
                }

                return xDocument.Root;
            }
        }

        protected const int COMMAND_TIMEOUT = 60000;

        protected static readonly List<int> ActiveEntities = new List<int>();

        protected CancellationTokenSource _cts;

        public string ConversationQueueName => string.Format("TradeCoordinator_ListenerQueue_{0}", this.Identity);

        public string ConversationServiceName => string.Format("TradeCoordinator_ListenerService_{0}", this.Identity);

        public string InstallListenerProcedureName => string.Format("TradeCoordinator_InstallListener_{0}", this.Identity);

        public string UninstallListenerProcedureName => string.Format("TradeCoordinator_UninstallListener_{0}", this.Identity);

        public static async Task<int[]> GetDependencyDbIdentities(string connectionString, string database)
        {
            if (connectionString == null)
                throw new ArgumentNullException("connectionString");

            if (database == null)
                throw new ArgumentNullException("database");

            List<string> result = new List<string>();

            using (SqlConnection connection = new SqlConnection(connectionString))
            using (SqlCommand command = connection.CreateCommand())
            {
                await connection.OpenAsync();
                command.CommandText = string.Format(SQL_FORMAT_GET_DEPENDENCY_IDENTITIES, database);
                command.CommandType = CommandType.Text;

                using (SqlDataReader reader = await command.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                        result.Add(reader.GetString(0));
                }
            }

            return result.Select(p => int.TryParse(p, out int temp) ? temp : -1).Where(p => p != -1).ToArray();
        }

        public async Task CleanDatabase(string connectionString, string database)
        {
            await ExecuteNonQuery(string.Format(SQL_FORMAT_FORCED_DATABASE_CLEANING, database), connectionString);
        }

        protected async Task<int?> ExecuteNonQuery(string commandText, string connectionString)
        {
            int? result = null;
            try
            {                
                using (SqlConnection conn = new SqlConnection(connectionString))
                using (SqlCommand command = new SqlCommand(commandText, conn))
                {
                    await conn.OpenAsync();
                    command.CommandType = CommandType.Text;

                    result = !commandText.Contains("INSERT") && !commandText.Contains("UPDATE") && !commandText.Contains("DELETE")
                        ? (int?)await command.ExecuteScalarAsync()
                        : await command.ExecuteNonQueryAsync();
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Database={DatabaseName} Schema={SchemaName} Table={TableName} Identity={Identity} " +
                                     $"Queue={ConversationQueueName} Service={ConversationServiceName} Trace={ex}");
            }
            return result;
        }

        protected async Task<string> ReceiveEvent(CancellationToken ct)
        {
            string res = string.Empty;

            var commandText = string.Format(
                SQL_FORMAT_RECEIVE_EVENT,
                this.DatabaseName,
                this.ConversationQueueName,
                COMMAND_TIMEOUT / 2,
                this.SchemaName);

            using (SqlConnection conn = new SqlConnection(this.ConnectionString))
            using (SqlCommand command = new SqlCommand(commandText, conn))
            {
                await conn.OpenAsync(ct);
                command.CommandType = CommandType.Text;
                command.CommandTimeout = COMMAND_TIMEOUT;
                using (var reader = await command.ExecuteReaderAsync(ct))
                {
                    res = !await reader.ReadAsync(ct) || await reader.IsDBNullAsync(0, ct) ? string.Empty : reader.GetString(0);
                }
            }

            return res;
        }

        public void Dispose()
        {
            Stop();
        }

        public event Func<object, TableChangedEventArgs, Task> TableChanged;

        protected async Task OnTableChanged(string message)
        {
            Func<object, TableChangedEventArgs, Task> evnt = this.TableChanged;
            if (evnt == null) return;

            await evnt.Invoke(this, new TableChangedEventArgs(message));
            MessagesProcessed++;
        }

        public event Func<object, Task> NotificationProcessStopped;

        protected async Task OnNotificationProcessStopped()
        {
            Func<object, Task> evnt = NotificationProcessStopped;
            if (evnt == null) return;

            await evnt.Invoke(this);
        }

        public override bool Start()
        {

            lock (ActiveEntities)
            {
                if (!ActiveEntities.Contains(this.Identity))
                {
                    ActiveEntities.Add(this.Identity);
                }
            }
            try
            {

                _cts = new CancellationTokenSource();
                CancellationToken ct = _cts.Token;
                Active = true;
                Task.Factory.StartNew(async () =>
                {
                    try
                    {
                        while (!ct.IsCancellationRequested)
                        {
                            var message = await ReceiveEvent(ct);

                            Active = true;
                            if (!string.IsNullOrWhiteSpace(message))
                                await OnTableChanged(message);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, ex.Message);
                    }
                    finally
                    {
                        Active = false;
                        await OnNotificationProcessStopped();
                        throw new Exception("Service Broker Listener has stopped. Restarting...");
                    }
                }, _cts.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);
            }
            catch
            {
                Start();
            }

            return true;
        }

        public override bool Stop()
        {
            lock (ActiveEntities)
                if (ActiveEntities.Contains(Identity)) ActiveEntities.Remove(Identity);

            if ((_cts == null) || _cts.Token.IsCancellationRequested)
                return true;

            if (!_cts.Token.CanBeCanceled)
                return true;

            _cts.Cancel();
            return true;
        }


    }

    /// <summary>Trigger based SQL Servicer Broker Listener
    /// <para>Creates a trigger on the database table for adding messages to the service broker queue</para>
    /// </summary>
    /// <typeparam name="T">Message Type To Deserialize To</typeparam>
    public sealed class ServiceBrokerListener<T> : ServiceBrokerListenerBase<T>, IDisposable where T : class
    {

        #region Scripts

        #region Procedures       

        /*
         T-SQL script-template which creates notification setup procedure.
         {0} - database name.
         {1} - setup procedure name.
         {2} - service broker configuration statement.
         {3} - notification trigger configuration statement.
         {4} - notification trigger check statement.         
         {5} - schema name.
         {6} - List of columns to select (with appropriate casts for datatypes)         
         */
        private const string SQL_FORMAT_CREATE_INSTALLATION_PROCEDURE = @"
                USE [{0}]
                " + SQL_PERMISSIONS_INFO + @"
                IF OBJECT_ID ('{5}.{1}', 'P') IS NULL
                BEGIN
                    EXEC ('
                        CREATE PROCEDURE {5}.{1}
                        AS
                        BEGIN
                            -- Service Broker configuration statement.
                            {2}
                            -- Notification Trigger check statement.
                            {4}
                            -- Notification Trigger configuration statement.
                            DECLARE @triggerStatement NVARCHAR(MAX)
                            DECLARE @select NVARCHAR(MAX)
                            DECLARE @sqlInserted NVARCHAR(MAX)
                            DECLARE @sqlDeleted NVARCHAR(MAX)
                            
                            SET @triggerStatement = N''{3}''
                            
                            SET @select = {6}
                            SET @sqlInserted = 
                                N''SET @retvalOUT = (SELECT '' + @select + N'' 
                                                     FROM INSERTED 
                                                     FOR XML PATH(''''row''''), ROOT (''''inserted''''))''
                            SET @sqlDeleted = 
                                N''SET @retvalOUT = (SELECT '' + @select + N'' 
                                                     FROM DELETED 
                                                     FOR XML PATH(''''row''''), ROOT (''''deleted''''))''                            
                            SET @triggerStatement = REPLACE(@triggerStatement
                                                     , ''%inserted_select_statement%'', @sqlInserted)
                            SET @triggerStatement = REPLACE(@triggerStatement
                                                     , ''%deleted_select_statement%'', @sqlDeleted)
                            EXEC sp_executesql @triggerStatement
                        END
                        ')
                END
            ";

        /*
         T-SQL script-template which creates notification uninstall procedure.
         {0} - database name.
         {1} - uninstall procedure name.
         {2} - notification trigger drop statement.
         {3} - service broker uninstall statement.
         {4} - schema name.
         {5} - install procedure name.        
        */
        private const string SQL_FORMAT_CREATE_UNINSTALLATION_PROCEDURE = @"
                USE [{0}]
                " + SQL_PERMISSIONS_INFO + @"
                IF OBJECT_ID ('{4}.{1}', 'P') IS NULL
                BEGIN
                    EXEC ('
                        CREATE PROCEDURE {4}.{1}
                        AS
                        BEGIN
                            -- Notification Trigger drop statement.
                            {3}
                            -- Service Broker uninstall statement.
                            {2}
                            IF OBJECT_ID (''{4}.{5}'', ''P'') IS NOT NULL
                                DROP PROCEDURE {4}.{5}
                            
                            DROP PROCEDURE {4}.{1}
                        END
                        ')
                END
            ";

        #endregion        

        #region Notification Trigger

        /*
         T-SQL script-template which creates notification trigger.
         {0} - notification trigger name. 
         {1} - schema name.
        */
        private const string SQL_FORMAT_DELETE_NOTIFICATION_TRIGGER = @"
                IF OBJECT_ID ('{1}.{0}', 'TR') IS NOT NULL
                    DROP TRIGGER {1}.[{0}];
            ";

        private const string SQL_FORMAT_CHECK_NOTIFICATION_TRIGGER = @"
                IF OBJECT_ID ('{1}.{0}', 'TR') IS NOT NULL
                    RETURN;
            ";

        /*
         T-SQL script-template which creates notification trigger.
         {0} - monitorable table name.
         {1} - notification trigger name.
         {2} - event data (INSERT, DELETE, UPDATE...).
         {3} - conversation service name. 
         {4} - detailed changes tracking mode.
         {5} - schema name.
         {6} - Trigger if statements
         {7} - ending statement for trigger if
         %inserted_select_statement% - sql code which sets trigger "inserted" value to @retvalOUT variable.
         %deleted_select_statement% - sql code which sets trigger "deleted" value to @retvalOUT variable.
        */
        private const string SQL_FORMAT_CREATE_NOTIFICATION_TRIGGER = @"
                CREATE TRIGGER [{1}]
                ON {5}.[{0}]
                WITH EXECUTE AS OWNER
                AFTER {2} 
                AS
                SET NOCOUNT ON;
                --Trigger {0} is rising...
                IF EXISTS (SELECT * FROM sys.services WHERE name = '{3}')
                BEGIN
                {6}
                    DECLARE @message NVARCHAR(MAX)
                    SET @message = N'<root/>'
                    IF ({4} EXISTS(SELECT 1))
                    BEGIN
                        DECLARE @retvalOUT NVARCHAR(MAX)
                        %inserted_select_statement%
                        IF (@retvalOUT IS NOT NULL)
                        BEGIN SET @message = N'<root>' + @retvalOUT END                        
                        %deleted_select_statement%
                        IF (@retvalOUT IS NOT NULL)
                        BEGIN
                            IF (@message = N'<root/>') BEGIN SET @message = N'<root>' + @retvalOUT END
                            ELSE BEGIN SET @message = @message + @retvalOUT END
                        END 
                        IF (@message != N'<root/>') BEGIN SET @message = @message + N'</root>' END
                    END
                    {7}
                	--Beginning of dialog...
                	DECLARE @ConvHandle UNIQUEIDENTIFIER
                	--Determine the Initiator Service, Target Service and the Contract 
                	BEGIN DIALOG @ConvHandle 
                        FROM SERVICE [{3}] TO SERVICE '{3}' ON CONTRACT [DEFAULT] WITH ENCRYPTION=OFF, LIFETIME = 86400; 
	                --Send the Message
	                SEND ON CONVERSATION @ConvHandle MESSAGE TYPE [DEFAULT] (@message);
	                --End conversation
	                END CONVERSATION @ConvHandle;
                END
            ";

        #endregion                

        #endregion        

        public string ConversationTriggerName => string.Format("TradeCoordinator_Listener_{0}", this.Identity);

        private string InsertedPredicate = "";
        private string UpdatedPredicate = "";
        private string DeletedPredicate = "";

        public ServiceBrokerListener(
            ILogger<ServiceBrokerListener> logger,
            string connectionString,
            string databaseName,
            string tableName,
            string schemaName = "dbo",
            NotificationTypes listenerType =
                NotificationTypes.Insert | NotificationTypes.Update | NotificationTypes.Delete,
            bool receiveDetails = true, int identity = 1)
        {
            _logger = logger;
            ConnectionString = connectionString;
            DatabaseName = databaseName;
            TableName = tableName;
            SchemaName = schemaName;
            NotificaionTypes = listenerType;
            DetailsIncluded = receiveDetails;
            Identity = identity;
            SetString();
            MessagesProcessed = 0;

        }

        private void SetString()
        {
            var properties = typeof(T).GetProperties().Where(x=> x.GetCustomAttributes(true).OfType<ExcludeFromSelect>().FirstOrDefault() == null);

            if (properties.Any(x => x.GetCustomAttributes(true).Any(y => (y as Table) != null)))
            {
                properties = properties.Where(x => x.GetCustomAttributes(true).Any(y => (y as Table)?.Name == TableName)).ToArray();
            }
            SelectList = "''";

            foreach (var prop in properties)
            {
                var attributeCheck = prop.GetCustomAttributes(true).OfType<Column>().FirstOrDefault();
                var tempString = "";
                var propertyType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
                if (propertyType == typeof(int))
                {
                    if (prop.GetCustomAttributes(true).OfType<IsSystem>().FirstOrDefault()?.IsSystemVariable == true)
                    {
                        tempString += "CAST(" + (attributeCheck != null ? attributeCheck.Name : prop.Name) + " as int) as [" + prop.Name + "],";
                    }
                    else
                    {
                        tempString += "CAST([" + (attributeCheck != null ? attributeCheck.Name : prop.Name) + "] as int) as [" + prop.Name + "],";
                    }
                }
                else if (propertyType == typeof(decimal) || propertyType == typeof(float) || propertyType == typeof(double))
                {
                    if (prop.GetCustomAttributes(true).OfType<IsSystem>().FirstOrDefault()?.IsSystemVariable == true)
                    {
                        tempString += "CAST(" + (attributeCheck != null ? attributeCheck.Name : prop.Name) + " as decimal) as [" + prop.Name + "],";
                    }
                    else
                    {
                        tempString += "CAST([" + (attributeCheck != null ? attributeCheck.Name : prop.Name) + "] as decimal) as [" + prop.Name + "],";
                    }
                }
                else if (propertyType.IsEnum)
                {
                    if (prop.GetCustomAttributes(true).OfType<IsSystem>().FirstOrDefault()?.IsSystemVariable == true)
                    {
                        tempString += "CAST(" + (attributeCheck != null ? attributeCheck.Name : prop.Name) + " as varchar(200)) as [" + prop.Name + "],";
                    }
                    else
                    {
                        tempString += "CAST([" + (attributeCheck != null ? attributeCheck.Name : prop.Name) + "] as varchar(200)) as [" + prop.Name + "],";
                    }
                }
                else
                {
                    if (prop.GetCustomAttributes(true).OfType<IsSystem>().FirstOrDefault()?.IsSystemVariable == true)
                    {
                        tempString += "" + (attributeCheck != null ? attributeCheck.Name : prop.Name) + " as [" + prop.Name + "],";
                    }
                    else
                    {
                        tempString += "[" + (attributeCheck != null ? attributeCheck.Name : prop.Name) + "] as [" + prop.Name + "],";
                    }
                }
                SelectList += tempString;
            }
            SelectList = SelectList.TrimEnd(',');
            SelectList += "''";
        }

        public override bool Install()
        {
            try
            {
                Task.Factory.StartNew(async () =>
                {
                    try
                    {
                        await this.InstallNotification();
                    }
                    catch (Exception e)
                    {
                        _logger.LogError($"{e.Message} \r\t {e.StackTrace}");
                        throw e;
                    }
                });
            }
            catch (Exception)
            {
                return false;
            }

            return true;
        }

        public override bool Uninstall()
        {
            Task.Factory.StartNew(async () => await UninstallNotification());

            lock (ActiveEntities)
                if (ActiveEntities.Contains(Identity)) ActiveEntities.Remove(Identity);

            if ((_cts == null) || _cts.Token.IsCancellationRequested)
                return true;

            if (!_cts.Token.CanBeCanceled)
                return true;

            _cts.Cancel();
            return true;
        }

        private string GetUninstallNotificationProcedureScript()
        {
            string uninstallServiceBrokerNotificationScript = string.Format(
                SQL_FORMAT_UNINSTALL_SERVICE_BROKER_NOTIFICATION,
                this.ConversationQueueName,
                this.ConversationServiceName,
                this.SchemaName);
            string uninstallNotificationTriggerScript = string.Format(
                SQL_FORMAT_DELETE_NOTIFICATION_TRIGGER,
                this.ConversationTriggerName,
                this.SchemaName);
            string uninstallationProcedureScript =
                string.Format(
                    SQL_FORMAT_CREATE_UNINSTALLATION_PROCEDURE,
                    this.DatabaseName,
                    this.UninstallListenerProcedureName,
                    uninstallServiceBrokerNotificationScript.Replace("'", "''"),
                    uninstallNotificationTriggerScript.Replace("'", "''"),
                    this.SchemaName,
                    this.InstallListenerProcedureName);
            return uninstallationProcedureScript;
        }

        private string GetInstallNotificationProcedureScript()
        {
            string predicate = "";

            if (!string.IsNullOrWhiteSpace(InsertedPredicate) || !string.IsNullOrWhiteSpace(UpdatedPredicate) || !string.IsNullOrWhiteSpace(DeletedPredicate))
            {
                predicate = @"IF(" + (!string.IsNullOrWhiteSpace(InsertedPredicate) ? InsertedPredicate : "") +
                    (!string.IsNullOrWhiteSpace(UpdatedPredicate) ? !string.IsNullOrWhiteSpace(InsertedPredicate) ? " OR " + UpdatedPredicate : UpdatedPredicate : "")
                    + (!string.IsNullOrWhiteSpace(DeletedPredicate) ? !string.IsNullOrWhiteSpace(UpdatedPredicate) || !string.IsNullOrWhiteSpace(InsertedPredicate) ? " OR " + DeletedPredicate : DeletedPredicate : "") + ")" +
                    "BEGIN";
            }

            string installServiceBrokerNotificationScript = string.Format(
                SQL_FORMAT_INSTALL_SEVICE_BROKER_NOTIFICATION,
                this.DatabaseName,
                this.ConversationQueueName,
                this.ConversationServiceName,
                this.SchemaName);
            string installNotificationTriggerScript =
                string.Format(
                    SQL_FORMAT_CREATE_NOTIFICATION_TRIGGER,
                    this.TableName,
                    this.ConversationTriggerName,
                    GetTriggerTypeByListenerType(),
                    this.ConversationServiceName,
                    this.DetailsIncluded ? string.Empty : @"NOT",
                    this.SchemaName,
                    !string.IsNullOrWhiteSpace(predicate) ? predicate : "",
                    !string.IsNullOrWhiteSpace(predicate) ? "END" : "");
            string uninstallNotificationTriggerScript =
                string.Format(
                    SQL_FORMAT_CHECK_NOTIFICATION_TRIGGER,
                    this.ConversationTriggerName,
                    this.SchemaName);
            string installationProcedureScript =
                string.Format(
                    SQL_FORMAT_CREATE_INSTALLATION_PROCEDURE,
                    this.DatabaseName,
                    this.InstallListenerProcedureName,
                    installServiceBrokerNotificationScript.Replace("'", "''"),
                    installNotificationTriggerScript.Replace("'", "''''"),
                    uninstallNotificationTriggerScript.Replace("'", "''"),
                    this.SchemaName,
                    this.SelectList);
            return installationProcedureScript;
        }

        private string GetTriggerTypeByListenerType()
        {
            StringBuilder result = new StringBuilder();
            if (this.NotificaionTypes.HasFlag(NotificationTypes.Insert))
                result.Append("INSERT");
            if (this.NotificaionTypes.HasFlag(NotificationTypes.Update))
                result.Append(result.Length == 0 ? "UPDATE" : ", UPDATE");
            if (this.NotificaionTypes.HasFlag(NotificationTypes.Delete))
                result.Append(result.Length == 0 ? "DELETE" : ", DELETE");
            if (result.Length == 0) result.Append("INSERT");

            return result.ToString();
        }

        private async Task UninstallNotification()
        {
            string execUninstallationProcedureScript = string.Format(
                SQL_FORMAT_EXECUTE_PROCEDURE,
                this.DatabaseName,
                this.UninstallListenerProcedureName,
                this.SchemaName);
            await ExecuteNonQuery(execUninstallationProcedureScript, this.ConnectionString);
        }

        private async Task InstallNotification()
        {
            string execInstallationProcedureScript = string.Format(
                SQL_FORMAT_EXECUTE_PROCEDURE,
                this.DatabaseName,
                this.InstallListenerProcedureName,
                this.SchemaName);
            await ExecuteNonQuery(GetInstallNotificationProcedureScript(), this.ConnectionString);
            await ExecuteNonQuery(GetUninstallNotificationProcedureScript(), this.ConnectionString);
            await ExecuteNonQuery(execInstallationProcedureScript, this.ConnectionString);
        }

        /// <summary>Adds "IF" condition to the trigger
        /// <para>Set up using a lambda that can handle basic primitive comparison, or using the FilterHelper class</para>
        /// </summary>
        /// <param name="type">Notification type for the condition - Insert/Update/Delete</param>
        /// <param name="conditions">Lamba / FilterHelpers</param>
        public void AddTriggerConditions(NotificationTypes type, Expression<Func<T, bool>> conditions)
        {
            var strings = TriggerPredicateBuilder.ToSql(type, conditions);

            switch (type)
            {
                case NotificationTypes.Insert:
                    {
                        if (!string.IsNullOrWhiteSpace(InsertedPredicate)) throw new Exception("Insert predicate already exists for this listener");

                        InsertedPredicate = "(EXISTS(SELECT * FROM INSERTED) AND NOT(EXISTS(SELECT * FROM DELETED)) AND" + strings + ")";
                        break;
                    }
                case NotificationTypes.Update:
                    {
                        if (!string.IsNullOrWhiteSpace(UpdatedPredicate)) throw new Exception("Update predicate already exists for this listener");

                        UpdatedPredicate = "(EXISTS(SELECT * FROM INSERTED) AND EXISTS(SELECT * FROM DELETED) AND " + strings + ")";
                        break;
                    }
                case NotificationTypes.Delete:
                    {
                        if (!string.IsNullOrWhiteSpace(DeletedPredicate)) throw new Exception("Delete predicate already exists for this listener");

                        DeletedPredicate = "(EXISTS(SELECT * FROM DELETED) AND NOT(EXISTS(SELECT * FROM INSERTED)) AND " + strings + ")";
                        break;
                    }
            }
        }
    }

    /// <summary>Stored Procedure based SQL Servicer Broker Listener
    /// <para>Creates a looping stored procedure to group rows and send them in batches</para>
    /// </summary>
    /// <typeparam name="T">Message Type To Deserialize To</typeparam>
    public sealed class ServiceBrokerBatchListener<T> : ServiceBrokerListenerBase<T>, IDisposable where T : class
    {

        #region Scripts

        #region Procedures

        /*
         T-SQL script-template which creates notification setup procedure.
         {0} - database name.
         {1} - setup procedure name.
         {2} - service broker configuration statement.
         {3} - notification proc configuration statement.
         {4} - notification proc check statement.         
         {5} - schema name.   
         {6} - status table creation
         {7} - stored procedure for starting the batch stored procedure
         {8} - check if sql side proc already exists
         {9} - service broker second queue setup
         */
        private const string SQL_FORMAT_CREATE_INSTALLATION_PROCEDURE = @"
                USE [{0}]
                " + SQL_PERMISSIONS_INFO + @"
                IF OBJECT_ID ('{5}.{1}', 'P') IS NULL
                BEGIN
                    EXEC ('
                        CREATE PROCEDURE {5}.{1}
                        AS
                        BEGIN
                            -- Service Broker configuration statement.
                            {2}
                            {9}
                            --Check Status Table And Create If Missing
                            {6}
                            -- Notification Proc check statement.
                            {4}                           
                            -- Notification Proc configuration statement.
                            DECLARE @longRunningBatchProc NVARCHAR(MAX)                            
                            DECLARE @select NVARCHAR(MAX)
                            DECLARE @sqlInserted NVARCHAR(MAX)
                            DECLARE @sqlDeleted NVARCHAR(MAX)
                            
                            SET @longRunningBatchProc =N'' {3} ''

                            EXEC sp_executesql @longRunningBatchProc

                            -- sql side Proc check statement.
                            {8}                           
                            -- Notification Proc configuration statement.
                            DECLARE @procToStartLongRunningProc NVARCHAR(MAX)
                            SET @procToStartLongRunningProc =N'' {7} ''
                            
                            EXEC sp_executesql @procToStartLongRunningProc
                        END
                        ')
                END
            ";

        /*
        T-SQL script-template which creates notification uninstall procedure.
        {0} - database name.
        {1} - uninstall procedure name.
        {2} - notification proc drop statement.
        {3} - service broker uninstall statement.
        {4} - schema name.
        {5} - install procedure name.   
        {6} - sql side notification name
        {7} - second queue name
       */
        private const string SQL_FORMAT_CREATE_UNINSTALLATION_PROCEDURE = @"
                USE [{0}]
                " + SQL_PERMISSIONS_INFO + @"
                IF OBJECT_ID ('{4}.{1}', 'P') IS NULL
                BEGIN
                    EXEC ('
                        CREATE PROCEDURE {4}.{1}
                        AS
                        BEGIN
                            -- Notification proc drop statement.
                            {3}
                            {7}
                            -- Service Broker uninstall statement.
                            {2}
                            {6}
                            IF OBJECT_ID (''{4}.{5}'', ''P'') IS NOT NULL
                                DROP PROCEDURE {4}.{5}
                            
                            DROP PROCEDURE {4}.{1}
                        END
                        ')
                END
            ";

        /*
        T-SQL script-template which creates notification uninstall procedure.
        {0} - schema name.
        {1} - status table name.        
       */
        private const string SQL_FORMAT_CREATE_STATUS_TABLE = @"
                IF NOT (EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{0}' 
                 AND  TABLE_NAME = '{1}'))
                BEGIN
                    CREATE TABLE {0}.{1}(
                    Id INT IDENTITY(1,1) PRIMARY KEY,
                    RunStatus INT)

                    INSERT INTO {0}.{1} (RunStatus) VALUES (0)
                END
            ";


        /*
       T-SQL script-template which creates notification uninstall procedure.
       {0} - Sql side queue.
       {1} - sql side procedure name.    
       {2} - identity. 
      */
        private const string SQL_FORMAT_CREATE_SQL_SIDE_PROC = @"
                create procedure {1}
                as
                begin
                set nocount on;
                declare @h uniqueidentifier
                    , @messageTypeName sysname
                    , @messageBody varbinary(max)
                    , @xmlBody xml
                    , @procedureName sysname
                    , @startTime datetime
                    , @finishTime datetime
                    , @execErrorNumber int
                    , @execErrorMessage nvarchar(2048)
                    , @xactState smallint

                begin transaction;
                begin try;
                    receive top(1) 
                        @h = [conversation_handle]
                        , @messageTypeName = [message_type_name]
                        , @messageBody = [message_body]
                        from [{0}];
                    if (@h is not null)
                    begin
                        if (@messageTypeName = N''DEFAULT'')
                        begin
                            -- The DEFAULT message type is a procedure invocation.
                            -- Extract the name of the procedure from the message body.
                            --
                            select @xmlBody = CAST(@messageBody as xml);
                            select @procedureName = @xmlBody.value(
                                ''(//procedure/name)[1]''
                                , ''sysname'');

                            save transaction Trade_{2}_procedure;
                            select @startTime = GETUTCDATE();
                            begin try
                                exec @procedureName;
                            end try
                            begin catch
                            -- This catch block tries to deal with failures of the procedure execution
                            -- If possible it rolls back to the savepoint created earlier, allowing
                            -- the activated procedure to continue. If the executed procedure 
                            -- raises an error with severity 16 or higher, it will doom the transaction
                            -- and thus rollback the RECEIVE. Such case will be a poison message,
                            -- resulting in the queue disabling.
                            --
                            select @execErrorNumber = ERROR_NUMBER(),
                                @execErrorMessage = ERROR_MESSAGE(),
                                @xactState = XACT_STATE();
                            if (@xactState = -1)
                            begin
                                rollback;
                                raiserror(N''Unrecoverable error in procedure %s: %i: %s'', 16, 10,
                                    @procedureName, @execErrorNumber, @execErrorMessage);
                            end
                            else if (@xactState = 1)
                            begin
                                rollback transaction Trade_{2}_procedure;
                            end
                            end catch

                            end conversation @h;
                        end 
                        else if (@messageTypeName = N''http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog'')
                        begin
                            end conversation @h;
                        end
                        else if (@messageTypeName = N''http://schemas.microsoft.com/SQL/ServiceBroker/Error'')
                        begin                           
                            end conversation @h;
                        end
                        else
                        begin
                            raiserror(N''Received unexpected message type: %s'', 16, 50, @messageTypeName);
                        end
                    end
                    commit;
                end try
                begin catch
                    declare @error int
                        , @message nvarchar(2048);
                    select @error = ERROR_NUMBER()
                        , @message = ERROR_MESSAGE()
                        , @xactState = XACT_STATE();
                    if (@xactState <> 0)
                    begin
                        rollback;
                    end;
                    raiserror(N''Error: %i, %s'', 1, 60,  @error, @message) with log;
                end catch
                end
            ";

        /*
         T-SQL script-template which removes database notification.
         {0} - conversation queue name.
         {1} - conversation service name.
         {2} - schema name.
        */
        private const string SQL_FORMAT_UNINSTALL_SERVICE_BROKER_NOTIFICATION_BATCH = @"
                
                SELECT @serviceId = service_id FROM sys.services 
                WHERE sys.services.name = '{1}'                
                DECLARE Conv CURSOR FOR
                SELECT CEP.conversation_handle FROM sys.conversation_endpoints CEP
                WHERE CEP.service_id = @serviceId AND ([state] != 'CD' OR [lifetime] > GETDATE() + 1)
                OPEN Conv;
                FETCH NEXT FROM Conv INTO @ConvHandle;
                WHILE (@@FETCH_STATUS = 0) BEGIN
    	            END CONVERSATION @ConvHandle WITH CLEANUP;
                    FETCH NEXT FROM Conv INTO @ConvHandle;
                END
                CLOSE Conv;
                DEALLOCATE Conv;
                -- Droping service and queue.
                DROP SERVICE [{1}];
                IF OBJECT_ID ('{2}.{0}', 'SQ') IS NOT NULL
	                DROP QUEUE {2}.[{0}];
            ";


        #endregion

        #region RunningProc

        /*
         T-SQL script-template which deletes notification proc.
         {0} - notification proc name.
        */
        private const string SQL_FORMAT_DELETE_NOTIFICATION_PROCEDURE = @"
                IF OBJECT_ID ('{0}', 'P') IS NOT NULL
                    DROP PROCEDURE [{0}];
            ";

        private const string SQL_FORMAT_CHECK_NOTIFICATION_PROCEDURE = @"
                IF OBJECT_ID ('{0}', 'P') IS NOT NULL
                    RETURN;
            ";        

        /*
          T-SQL script-template to check if procedure is currently running
          {0} - procedure name
        */
        private const string SQL_FORMAT_CHECK_PROCEDURE_CURRENT_STATUS = @"
                  Select COUNT(*) FROM
                    (SELECT * FROM sys.dm_exec_requests WHERE sql_handle IS NOT NULL AND command = 'WAITFOR') a 
                    CROSS APPLY  sys.dm_exec_sql_text(a.sql_handle) t 
                    WHERE t.text LIKE '%{0}%' ";

        /*
          T-SQL script-template to stop the procedure from running
          {0} - table name
        */
        private const string SQL_FORMAT_STOP_PROCEDURE = @"
            UPDATE {0} SET RunStatus = 0";

        /*
         T-SQL script-template to start the procedure
         {0} - table namee
       */
        private const string SQL_FORMAT_START_PROCEDURE = @"
            UPDATE {0} SET RunStatus = 1
           ";

        /*
          T-SQL script-template to check if script should be running
          {0} - table name
        */
        private const string SQL_FORMAT_CHECK_PROCEDURE_SHOULD_RUN = @"
            SELECT TOP(1) COUNT(*) FROM {0} WHERE RunStatus = 1";

        /*
         T-SQL script-template to inser into Sql side queue
         {0} - Queue name
         {1} - Procedure Name
         {2} - Service Name
         {3} - Long Running Proc Name
       */
        private const string SQL_FORMAT_ADD_MESSAGE_TO_QUEUE = @"
            declare @h uniqueidentifier
	        , @xmlBody xml

            begin dialog conversation @h
            from service [{2}]
            to service N'{2}', 'current database'
            with encryption = off;            
            select @xmlBody = (
            select '{3}' as [name]
            for xml path('procedure'), type);
            send on conversation @h (@xmlBody);

            alter queue {0}
            with activation (
            procedure_name = [{1}]
            , max_queue_readers = 1
            , execute as owner
            , status = on)";

        /* 
         T-SQL script-template which creates notification proc.
         {0} - procedure name.
         {1} - delay length.
         {2} - identity to make temp table unique.
         {3} - initial select list built from STUFF and GROUP attributes. 
         {4} - select list.
         {5} - listener service name
         {6} - delete from statement
         {7} - proc precedure status table - for stopping / starting to proc running
        */
        private const string SQL_FORMAT_CREATE_NOTIFICATION_PROCEDURE = @"
                CREATE PROCEDURE [{0}]                 
                AS
                BEGIN
                    SET NOCOUNT ON;
                    IF 1 = (SELECT TOP(1) RunStatus FROM {7})
                    BEGIN
                            DECLARE @Counter int = 1
                            DECLARE @xml as xml
                            
                            IF OBJECT_ID(''tempdb.dbo.#temp{2}'', ''U'') IS NOT NULL
                                DROP TABLE #temp{2}
                            
                            {3}

                            IF EXISTS(SELECT * FROM #temp{2})
                            BEGIN
                                DECLARE @TotalRows int = (SELECT COUNT(*) FROM #temp{2})
                                WHILE(@Counter <= @TotalRows)
                                BEGIN
                                    DECLARE @message NVARCHAR(MAX)
                                    SET @message = N''<root/>''
                                    DECLARE @retvalOUT NVARCHAR(MAX)
                                    SET @retvalOUT = ({4}
                                        FROM #temp{2} WHERE number = @Counter
                                        FOR XML PATH(''row''), ROOT (''inserted''))
                                    IF (@retvalOUT IS NOT NULL)
                                    BEGIN SET @message = N''<root>'' + @retvalOUT END
                                    IF (@message != N''<root/>'') BEGIN SET @message = @message + N''</root>'' END
                                    
                                    	--Beginning of dialog...
                	                DECLARE @ConvHandle UNIQUEIDENTIFIER
                	                    --Determine the Initiator Service, Target Service and the Contract 
                	                BEGIN DIALOG @ConvHandle 
                                        FROM SERVICE [{5}] TO SERVICE ''{5}'' ON CONTRACT [DEFAULT] WITH ENCRYPTION=OFF, LIFETIME = 86400; 
	                                    --Send the Message
	                                SEND ON CONVERSATION @ConvHandle MESSAGE TYPE [DEFAULT] (@message);
	                                    --End conversation
	                                END CONVERSATION @ConvHandle;

                                    SET @xml = cast((''<X>''+replace((SELECT TOP(1) Id from #temp{2} where number = @Counter),'','' ,''</X><X>'')+''</X>'') as xml)
                                    {6}
                                    SET @Counter = @Counter + 1
                                END
                            END
                        WAITFOR DELAY ''{1}''
                        declare @h uniqueidentifier
	                    , @xmlBody xml

                         begin dialog conversation @h
                         from service [{8}]
                         to service N'{8}', 'current database'
                         with encryption = off;            
                         select @xmlBody = (
                         select '{9}' as [name]
                         for xml path('procedure'), type);
                         send on conversation @h (@xmlBody);
                    END
                END  
            ";

        #endregion

        #endregion

        private string InitialSelectList;

        private int WaitFor;

        private string HavingClause;

        private string DeleteClause;

        public string ConversationProcedureName => string.Format("TradeCoordinator_Listener_{0}", this.Identity);

        public string SqlConversationProcedureName => string.Format("TradeCoordinator_SqlListener_{0}", this.Identity);

        public string SqlConversationQueueName => string.Format("TradeCoordinator_SqlListenerQueue_{0}", this.Identity);

        public string SqlConversationServiceName => string.Format("TradeCoordinator_SqlListenerService_{0}", this.Identity);

        private string WaitForTime => TimeSpan.FromSeconds(WaitFor).ToString(@"hh\:mm\:ss");

        /// <summary>Stops the stored procedure from running</summary>
        public bool StopProcedure()
        {
            try
            {
                Task.Factory.StartNew(async () => await StopProcedureCommand());
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>Starts the stored procedure running</summary>
        public bool StartProcedure()
        {
            try
            {
                Task.Factory.StartNew(async () => await StartProcedureCommand());
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>Modified status function for batch listener that also returns the status of the stored procedure on the database</summary>
        public (int identity, string tableName, bool active, bool procRunning, long processedCount) BatchStatus()
        {
            var running = Task.Factory.StartNew(async () => await CheckIfRunning()).Result.Result;

            var shouldBeRunning = Task.Factory.StartNew(async () => await CheckProcShouldRun()).Result.Result;

            if (running && !shouldBeRunning)
            {
                StopProcedure();
                running = false;
            }
            else if (!running && shouldBeRunning)
            {
                StartProcedure();
                running = true;
            }

            return (Identity, TableName, Active, running, MessagesProcessed);

        }

        /// <summary>Sets the "HAVING" sql clause for the stored procedure
        /// <para>Should be in the format of "HAVING X = Y" to insert a correct clause</para>
        /// </summary>
        public void SetHaving(string having)
        {
            HavingClause = having;
        }

        private string StatusTableName;
        
        public ServiceBrokerBatchListener(
            ILogger<ServiceBrokerListener> logger,
            string connectionString,
            string databaseName,
            string tableName,
            string statusTableName,
            string schemaName = "dbo",
            int waitFor = 30,
            int identity = 1)
        {
            _logger = logger;
            ConnectionString = connectionString;
            DatabaseName = databaseName;
            TableName = tableName;
            SchemaName = schemaName;
            StatusTableName = statusTableName;
            Identity = identity;
            SetString();                                   
            MessagesProcessed = 0;
            WaitFor = waitFor;
        }

        public override bool Install()
        {
            SetInitialSelect();
            SetDeleteClause();

            try
            {
                Task.Factory.StartNew(async () =>
                {
                    try
                    {
                        await this.InstallNotification();
                    }
                    catch (Exception e)
                    {
                        _logger.LogError($"{e.Message} \r\t {e.StackTrace}");
                        throw e;
                    }
                });
            }
            catch (Exception)
            {
                return false;
            }

            return true;
        }

        public override bool Uninstall()
        {
            StopProcedure();

            Task.Factory.StartNew(async () => await UninstallNotification());

            lock (ActiveEntities)
                if (ActiveEntities.Contains(Identity)) ActiveEntities.Remove(Identity);

            if ((_cts == null) || _cts.Token.IsCancellationRequested)
                return true;

            if (!_cts.Token.CanBeCanceled)
                return true;

            _cts.Cancel();
            return true;
        }

        private async Task StopProcedureCommand()
        {
            string execStopScript = string.Format(SQL_FORMAT_STOP_PROCEDURE, StatusTableName);

            await ExecuteNonQuery(execStopScript, this.ConnectionString);
        }

        private async Task StartProcedureCommand()
        {
            string execStartScript = string.Format(SQL_FORMAT_START_PROCEDURE, StatusTableName);
            string execAddToQueue = string.Format(SQL_FORMAT_ADD_MESSAGE_TO_QUEUE, SqlConversationQueueName, SqlConversationProcedureName, SqlConversationServiceName, ConversationProcedureName);

            if (await CheckIfRunning())
            {
                return;
            }
            await ExecuteNonQuery(execStartScript, this.ConnectionString);
            await ExecuteNonQuery(execAddToQueue, this.ConnectionString);
        }

        private async Task UninstallNotification()
        {
            string execUninstallationProcedureScript = string.Format(
                SQL_FORMAT_EXECUTE_PROCEDURE,
                this.DatabaseName,
                this.UninstallListenerProcedureName,
                this.SchemaName);
            await ExecuteNonQuery(execUninstallationProcedureScript, this.ConnectionString);
        }

        private async Task InstallNotification()
        {
            string execInstallationProcedureScript = string.Format(
                SQL_FORMAT_EXECUTE_PROCEDURE,
                this.DatabaseName,
                this.InstallListenerProcedureName,
                this.SchemaName);
            await ExecuteNonQuery(GetInstallNotificationProcedureScript(), this.ConnectionString);
            await ExecuteNonQuery(GetUninstallNotificationProcedureScript(), this.ConnectionString);
            await ExecuteNonQuery(execInstallationProcedureScript, this.ConnectionString);
        }

        private async Task<bool> CheckIfRunning()
        {
            string checkString = GetProcedureRunningScript();

            var x = await ExecuteNonQuery(checkString, this.ConnectionString);

            return x > 0;
        }

        private async Task<bool> CheckProcShouldRun()
        {
            string shouldRun = GetProcedureShouldRunScript();

            var x = await ExecuteNonQuery(shouldRun, this.ConnectionString);

            return x > 0;
        }

        private void SetString()
        {
            var properties = typeof(T).GetProperties().Where(x => x.GetCustomAttributes(true).OfType<ExcludeFromSelect>().FirstOrDefault() == null);

            SelectList = "SELECT ";

            foreach (var prop in properties)
            {
                var attributeCheck = prop.GetCustomAttributes(true).OfType<Column>().FirstOrDefault();
                var tempString = "";
                var propertyType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
                if (propertyType == typeof(int))
                {
                    if (prop.GetCustomAttributes(true).OfType<IsSystem>().FirstOrDefault()?.IsSystemVariable == true)
                    {
                        tempString += "CAST(" + (attributeCheck != null ? attributeCheck.Name : prop.Name) + " as int) as [" + prop.Name + "],";
                    }
                    else
                    {
                        tempString += "CAST([" + (attributeCheck != null ? attributeCheck.Name : prop.Name) + "] as int) as [" + prop.Name + "],";
                    }
                }
                else if (propertyType == typeof(decimal) || propertyType == typeof(float) || propertyType == typeof(double))
                {
                    if (prop.GetCustomAttributes(true).OfType<IsSystem>().FirstOrDefault()?.IsSystemVariable == true)
                    {
                        tempString += "CAST(" + (attributeCheck != null ? attributeCheck.Name : prop.Name) + " as decimal) as [" + prop.Name + "],";
                    }
                    else
                    {
                        tempString += "CAST([" + (attributeCheck != null ? attributeCheck.Name : prop.Name) + "] as decimal) as [" + prop.Name + "],";
                    }
                }
                else if (propertyType.IsEnum)
                {
                    if (prop.GetCustomAttributes(true).OfType<IsSystem>().FirstOrDefault()?.IsSystemVariable == true)
                    {
                        tempString += "CAST(" + (attributeCheck != null ? attributeCheck.Name : prop.Name) + " as varchar(200)) as [" + prop.Name + "],";
                    }
                    else
                    {
                        tempString += "CAST([" + (attributeCheck != null ? attributeCheck.Name : prop.Name) + "] as varchar(200)) as [" + prop.Name + "],";
                    }
                }
                else
                {
                    if (prop.GetCustomAttributes(true).OfType<IsSystem>().FirstOrDefault()?.IsSystemVariable == true)
                    {
                        tempString += "" + (attributeCheck != null ? attributeCheck.Name : prop.Name) + " as [" + prop.Name + "],";
                    }
                    else
                    {
                        tempString += "[" + (attributeCheck != null ? attributeCheck.Name : prop.Name) + "] as [" + prop.Name + "],";
                    }
                }
                SelectList += tempString;
            }
            SelectList = SelectList.TrimEnd(',');
            SelectList += "";
        }       

        private void SetInitialSelect()
        {
            var properties = typeof(T).GetProperties();

            InitialSelectList = "SELECT IDENTITY(int,1,1) as number, ";
            var stuffString = "";
            var groupstring = "";
            foreach (var groupProp in properties.Where(x => x.GetCustomAttributes(true).OfType<GroupBy>().FirstOrDefault() != null))
            {
                var attributeCheck1 = groupProp.GetCustomAttributes(true).OfType<Column>().FirstOrDefault()?.Name;
                var nameToUse1 = string.IsNullOrWhiteSpace(attributeCheck1) ? groupProp.Name : attributeCheck1;
                stuffString += "b." + nameToUse1 + " = a." + nameToUse1 + " AND ";
                groupstring += nameToUse1 + ",";
            }
            if (!string.IsNullOrWhiteSpace(stuffString))
            {
                stuffString = stuffString.Substring(0, stuffString.Length - 5) + " ";
            }
            


            foreach (var prop in properties)
            {
                var tempString = "";
                var propertyType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
                var attributeCheck = prop.GetCustomAttributes(true).OfType<Column>().FirstOrDefault()?.Name;
                var nameToUse = string.IsNullOrWhiteSpace(attributeCheck) ? prop.Name : attributeCheck; 

                if (prop.GetCustomAttributes(true).OfType<SqlStuff>().FirstOrDefault()?.SqlStuffVariable == true)
                {
                    tempString += "[" + nameToUse + "] = STUFF((SELECT '','' + CAST([" + nameToUse + "] as varchar(MAX)) FROM " + TableName + " b WHERE " + stuffString + "FOR XML PATH('''')), 1, 1, ''''),"; 
                }
                else
                {

                    if (prop.GetCustomAttributes(true).OfType<IsSystem>().FirstOrDefault()?.IsSystemVariable == true)
                    {
                        tempString += nameToUse + " as [" + prop.Name + "],";
                    }
                    else
                    {
                        tempString += "[" + nameToUse + "] as [" + prop.Name + "],";
                    }
                }
                                
                InitialSelectList += tempString;
            }
            InitialSelectList = InitialSelectList.TrimEnd(',');

            InitialSelectList += " INTO #temp" + Identity + " FROM " + TableName + " a GROUP BY " + groupstring.TrimEnd(',') + " " + HavingClause;
        }

        private void SetDeleteClause()
        {
            var property = typeof(T).GetProperties().FirstOrDefault(x=> x.GetCustomAttributes(true).OfType<DeleteOn>().FirstOrDefault() != null);
         
            DeleteClause = "DELETE FROM " + TableName + " WHERE " + (property.GetCustomAttributes(true).OfType<Column>().FirstOrDefault() == null ? property.Name : property.GetCustomAttributes(true).OfType<Column>().First().Name) + " IN (SELECT N.value(''.'', ''varchar(100)'') as value FROM @xml.nodes(''X'') as T(N))";
        }

        private string GetUninstallNotificationProcedureScript()
        {
            string uninstallServiceBrokerNotificationScript = string.Format(
                SQL_FORMAT_UNINSTALL_SERVICE_BROKER_NOTIFICATION,
                this.ConversationQueueName,
                this.ConversationServiceName,
                this.SchemaName);

            string uninstallSqlSideServiceBrokerNotificationScript = string.Format(
                SQL_FORMAT_UNINSTALL_SERVICE_BROKER_NOTIFICATION_BATCH,
                this.SqlConversationQueueName,
                this.SqlConversationServiceName,
                this.SchemaName);

            string uninstallNotificationProcScript = string.Format(
                SQL_FORMAT_DELETE_NOTIFICATION_PROCEDURE,
                this.SqlConversationProcedureName);

            string uninstallSqlSideNotificationProcScript = string.Format(
                SQL_FORMAT_DELETE_NOTIFICATION_PROCEDURE,
                this.ConversationProcedureName);

            string uninstallationProcedureScript =
                string.Format(
                    SQL_FORMAT_CREATE_UNINSTALLATION_PROCEDURE,
                    this.DatabaseName,
                    this.UninstallListenerProcedureName,
                    uninstallServiceBrokerNotificationScript.Replace("'", "''"),
                    uninstallNotificationProcScript.Replace("'", "''"),
                    this.SchemaName,
                    this.InstallListenerProcedureName,
                    uninstallSqlSideServiceBrokerNotificationScript.Replace("'", "''"),
                    uninstallSqlSideNotificationProcScript.Replace("'", "''"));
            return uninstallationProcedureScript;
        }

        private string GetProcedureRunningScript()
        {
            return string.Format(SQL_FORMAT_CHECK_PROCEDURE_CURRENT_STATUS,
                ConversationProcedureName);
        }

        private string GetProcedureShouldRunScript()
        {
            return string.Format(SQL_FORMAT_CHECK_PROCEDURE_SHOULD_RUN,
                StatusTableName);
        }

        private string GetInstallNotificationProcedureScript()
        {
            string createStatusTableIfMissing = string.Format(
                SQL_FORMAT_CREATE_STATUS_TABLE,
                this.SchemaName,
                this.StatusTableName);

            string installServiceBrokerNotificationScript = string.Format(
                SQL_FORMAT_INSTALL_SEVICE_BROKER_NOTIFICATION,
                this.DatabaseName,
                this.ConversationQueueName,
                this.ConversationServiceName,
                this.SchemaName);

            string installSqlSideServiceBrokerNotificationScript = string.Format(
                SQL_FORMAT_INSTALL_SEVICE_BROKER_NOTIFICATION,
                this.DatabaseName,
                this.SqlConversationQueueName,
                this.SqlConversationServiceName,
                this.SchemaName);

            string installNotificationProcedureScript =
                string.Format(
                    SQL_FORMAT_CREATE_NOTIFICATION_PROCEDURE,
                    ConversationProcedureName,
                    WaitForTime,
                    this.Identity,
                    this.InitialSelectList,
                    this.SelectList,
                    this.ConversationServiceName,
                    this.DeleteClause,
                    StatusTableName,
                    this.SqlConversationServiceName,
                    this.SqlConversationProcedureName);

            string installSqlNotificationProcedureScript =
                string.Format(
                    SQL_FORMAT_CREATE_SQL_SIDE_PROC,
                    SqlConversationQueueName,
                    SqlConversationProcedureName,
                    this.Identity);

            string checkNotificationProcScript =
                string.Format(
                    SQL_FORMAT_CHECK_NOTIFICATION_PROCEDURE,
                    this.ConversationProcedureName);

            string checkSqlNotificationProcScript =
                string.Format(
                    SQL_FORMAT_CHECK_NOTIFICATION_PROCEDURE,
                    SqlConversationProcedureName);

            string installationProcedureScript =
                string.Format(
                    SQL_FORMAT_CREATE_INSTALLATION_PROCEDURE,
                    this.DatabaseName,
                    this.InstallListenerProcedureName,
                    installServiceBrokerNotificationScript.Replace("'", "''"),
                    installNotificationProcedureScript.Replace("'", "''"),
                    checkNotificationProcScript.Replace("'", "''"),
                    this.SchemaName,
                    createStatusTableIfMissing.Replace("'", "''"),
                    installSqlNotificationProcedureScript.Replace("'", "''"),
                    checkSqlNotificationProcScript.Replace("'", "''"),
                    installSqlSideServiceBrokerNotificationScript.Replace("'", "''"));

            return installationProcedureScript;
        }

    }

    /// <summary>Static class for additional functionality in the trigger predicate function</summary>
    public static class FilterHelpers
    {
        /// <summary>Creates an if condition for when the supplied object has changed on an update</summary>
        public static bool WhereHasChanged(object type)
        {
            return true;
        }

        /// <summary>Creates a capability to check update and delete types easily</summary>
        public static bool WhereIs(NotificationTypes type, object property, object value)
        {
            return true;
        }
    }

    public static class TriggerPredicateBuilder
    {
        public static string ToSql<T>(NotificationTypes type, Expression<Func<T, bool>> expression)
        {
            return RecurseThroughExpression(type, expression.Body, true);
        }

        private static string RecurseThroughExpression(NotificationTypes type, Expression expression, bool isUnary = false, bool quote = true)
        {
            if (expression is UnaryExpression unaryExpression)
            {
                var right = RecurseThroughExpression(type, unaryExpression.Operand, true);
                return "(" + NodeTypeToString(unaryExpression.NodeType, right == "NULL") + " " + right + ")";
            }
            if (expression is BinaryExpression binaryExpression)
            {
                var right = RecurseThroughExpression(type, binaryExpression.Right);
                return "(" + RecurseThroughExpression(type, binaryExpression.Left) + " " + NodeTypeToString(binaryExpression.NodeType, right == "NULL") + " " + right + ")";
            }
            if (expression is ConstantExpression constantExpression)
            {
                return ValueToString(constantExpression.Value, isUnary, quote);
            }
            if (expression is MemberExpression member)
            {
                if (member.Member is PropertyInfo property)
                {
                    var colName = property.GetCustomAttributes(true).OfType<Column>().FirstOrDefault() != null ? property.GetCustomAttributes(true).OfType<Column>().First().Name : property.Name;
                    return isUnary && member.Type == typeof(bool)
                        ? "((SELECT[" + colName + "] FROM " + NotificationTypeToString(type) + ") = 1)"
                        : "(SELECT[" + colName + "] FROM " + NotificationTypeToString(type) + ")";
                }
                if (member.Member is FieldInfo field)
                {
                    var colName = field.GetCustomAttributes(true).OfType<Column>().FirstOrDefault() != null ? field.GetCustomAttributes(true).OfType<Column>().First().Name : field.Name;
                    return isUnary && member.Type == typeof(bool)
                        ? "((SELECT[" + colName + "] FROM " + NotificationTypeToString(type) + ") = 1)"
                        : "(SELECT[" + colName + "] FROM " + NotificationTypeToString(type) + ")";
                }
                throw new Exception($"Expression does not refer to a property or field: {expression}");
            }
            if (expression is MethodCallExpression methodCall)
            {

                //Member partial match queries
                if (methodCall.Method == typeof(string).GetMethod("Contains", new[] { typeof(string) }))
                {
                    return "(" + RecurseThroughExpression(type, methodCall.Object) + " LIKE '%" + RecurseThroughExpression(type, methodCall.Arguments[0], quote: false) + "%')";
                }
                if (methodCall.Method == typeof(string).GetMethod("StartsWith", new[] { typeof(string) }))
                {
                    return "(" + RecurseThroughExpression(type, methodCall.Object) + " LIKE '" + RecurseThroughExpression(type, methodCall.Arguments[0], quote: false) + "%')";
                }
                if (methodCall.Method == typeof(string).GetMethod("EndsWith", new[] { typeof(string) }))
                {
                    return "(" + RecurseThroughExpression(type, methodCall.Object) + " LIKE '%" + RecurseThroughExpression(type, methodCall.Arguments[0], quote: false) + "')";
                }
                //Member contained in:
                if (methodCall.Method.Name == "Contains")
                {
                    Expression collection;
                    Expression property;
                    if (methodCall.Method.IsDefined(typeof(ExtensionAttribute)) && methodCall.Arguments.Count == 2)
                    {
                        collection = methodCall.Arguments[0];
                        property = methodCall.Arguments[1];
                    }
                    else if (!methodCall.Method.IsDefined(typeof(ExtensionAttribute)) && methodCall.Arguments.Count == 1)
                    {
                        collection = methodCall.Object;
                        property = methodCall.Arguments[0];
                    }
                    else
                    {
                        throw new Exception("Unsupported method call: " + methodCall.Method.Name);
                    }
                    var values = (IEnumerable<object>)GetValue(collection);
                    var concated = "";
                    foreach (var e in values)
                    {
                        concated += ValueToString(e, false, true) + ", ";
                    }
                    return concated == ""
                        ? ValueToString(false, true, false)
                        : "(" + RecurseThroughExpression(type, property) + " IN (" + concated.Substring(0, concated.Length - 2) + "))";
                }
                //FilterHelper Methods
                if (methodCall.Method.Name == "WhereHasChanged")
                {
                    if (type != NotificationTypes.Update)
                    {
                        throw new Exception("Method only supported for Update: " + methodCall.Method.Name);
                    }
                    return "(" + RecurseThroughExpression(type, ((UnaryExpression)methodCall.Arguments[0]).Operand) + " <> " + RecurseThroughExpression(NotificationTypes.Delete, ((UnaryExpression)methodCall.Arguments[0]).Operand) + ")";
                }
                if (methodCall.Method.Name == "WhereIs")
                {
                    var methodType = (NotificationTypes)((ConstantExpression)methodCall.Arguments[0]).Value;
                    if ((type == NotificationTypes.Insert && methodType == NotificationTypes.Insert) || (type == NotificationTypes.Delete && methodType == NotificationTypes.Delete) ||
                        (type == NotificationTypes.Update && methodType != NotificationTypes.None))
                    {
                        return "(" + RecurseThroughExpression(type, methodCall.Arguments[1]) + " = " + RecurseThroughExpression(methodType, methodCall.Arguments[2]) + ")";
                    }
                    throw new Exception($"Notification Type {methodType.ToString()} is not valid for {type.ToString()} predicates");
                }
                throw new Exception("Unsupported method call: " + methodCall.Method.Name);
            }
            throw new Exception("Unsupported expression: " + expression.GetType().Name);
        }

        private static string ValueToString(object value, bool isUnary, bool quote)
        {
            return value is bool
                ? isUnary ? (bool)value ? "(1=1)" : "(1=0)" : (bool)value ? "1" : "0"
                : (quote ? "'" : "") + value.ToString() + (quote ? "'" : "");
        }

        private static bool IsEnumerableType(Type type)
        {
            return type
                .GetInterfaces()
                .Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>));
        }

        private static object GetValue(Expression member)
        {
            // source: http://stackoverflow.com/a/2616980/291955
            var objectMember = Expression.Convert(member, typeof(object));
            var getterLambda = Expression.Lambda<Func<object>>(objectMember);
            var getter = getterLambda.Compile();
            return getter();
        }

        private static object NotificationTypeToString(NotificationTypes type)
        {
            switch (type)
            {
                case NotificationTypes.Delete:
                    return "DELETED";
                case NotificationTypes.Insert:
                    return "INSERTED";
                case NotificationTypes.Update:
                    return "INSERTED";
            }
            throw new Exception("Cannot create a filter with no type");
        }

        private static object NodeTypeToString(ExpressionType nodeType, bool rightIsNull)
        {
            switch (nodeType)
            {
                case ExpressionType.Add:
                    return "+";
                case ExpressionType.And:
                    return "&";
                case ExpressionType.AndAlso:
                    return "AND";
                case ExpressionType.Divide:
                    return "/";
                case ExpressionType.Equal:
                    return rightIsNull ? "IS" : "=";
                case ExpressionType.ExclusiveOr:
                    return "^";
                case ExpressionType.GreaterThan:
                    return ">";
                case ExpressionType.GreaterThanOrEqual:
                    return ">=";
                case ExpressionType.LessThan:
                    return "<";
                case ExpressionType.LessThanOrEqual:
                    return "<=";
                case ExpressionType.Modulo:
                    return "%";
                case ExpressionType.Multiply:
                    return "*";
                case ExpressionType.Negate:
                    return "-";
                case ExpressionType.Not:
                    return "NOT";
                case ExpressionType.NotEqual:
                    return "<>";
                case ExpressionType.Or:
                    return "|";
                case ExpressionType.OrElse:
                    return "OR";
                case ExpressionType.Subtract:
                    return "-";
            }
            throw new Exception($"Unsupported node type: {nodeType}");
        }
    }
}