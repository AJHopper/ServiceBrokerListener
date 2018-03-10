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

    public class Table : Attribute
    {
        private string name;

        public virtual string Name
        {
            get { return name; }
        }

        public Table(string TableName)
        {
            name = TableName;
        }

    }

    public class IsSystem : Attribute
    {
        private bool isSystem;

        public virtual bool IsSystemVariable
        {
            get { return isSystem; }
        }

        public IsSystem(bool IsSystem)
        {
            isSystem = IsSystem;
        }

    }

    public class Column : Attribute
    {
        private string name;

        public virtual string Name
        {
            get { return name; }
        }

        public Column(string ColumnName)
        {
            name = ColumnName;
        }

    }


    public abstract class SqlDepenecyEx
    {
        public abstract (string tableName, bool active) status();
    }

    public sealed class SqlDependencyEx<T> : SqlDepenecyEx, IDisposable where T : class
    {
        public override (string tableName, bool active) status()
        {
            return (TableName, Active);
        }

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

                HeldData = (Inserted, Deleted);
            }

            private (T Inserted, T Deleted) HeldData;

            public (T Inserted, T Deleted) Data
            {
                get
                {
                    return HeldData;
                }
            }

            public NotificationTypes NotificationType
            {
                get
                {
                    return Data.Inserted != null ? Data.Deleted != null ? NotificationTypes.Update : NotificationTypes.Insert :
                        Data.Deleted != null ? NotificationTypes.Delete : NotificationTypes.None;
                }
            }

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

        #region Scripts

        #region Procedures

        private const string SQL_PERMISSIONS_INFO = @"
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

        /*
         T-SQL script-template which creates notification setup procedure.
         {0} - database name.
         {1} - setup procedure name.
         {2} - service broker configuration statement.
         {3} - notification trigger configuration statement.
         {4} - notification trigger check statement.
         {5} - table name.
         {6} - schema name.
         {7} - List of columns to select (with appropriate casts for datatypes)         
         */
        private const string SQL_FORMAT_CREATE_INSTALLATION_PROCEDURE = @"
                USE [{0}]
                " + SQL_PERMISSIONS_INFO + @"
                IF OBJECT_ID ('{6}.{1}', 'P') IS NULL
                BEGIN
                    EXEC ('
                        CREATE PROCEDURE {6}.{1}
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
                            
                            SET @select = {7}
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

        #region ServiceBroker notification

        /*
         T-SQL script-template which prepares database for ServiceBroker notification.
         {0} - database name;
         {1} - conversation queue name.
         {2} - conversation service name.
         {3} - schema name.        
        */
        private const string SQL_FORMAT_INSTALL_SEVICE_BROKER_NOTIFICATION = @"
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
        private const string SQL_FORMAT_UNINSTALL_SERVICE_BROKER_NOTIFICATION = @"
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
                        FROM SERVICE [{3}] TO SERVICE '{3}' ON CONTRACT [DEFAULT] WITH ENCRYPTION=OFF, LIFETIME = 60; 
	                --Send the Message
	                SEND ON CONVERSATION @ConvHandle MESSAGE TYPE [DEFAULT] (@message);
	                --End conversation
	                END CONVERSATION @ConvHandle;
                END
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
        private const string SQL_FORMAT_RECEIVE_EVENT = @"
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
        private const string SQL_FORMAT_EXECUTE_PROCEDURE = @"
                USE [{0}]
                IF OBJECT_ID ('{2}.{1}', 'P') IS NOT NULL
                    EXEC {2}.{1}
            ";

        /* <summary>
         T-SQL script-template which returns all dependency identities in the database.
         {0} - database name.
        */
        private const string SQL_FORMAT_GET_DEPENDENCY_IDENTITIES = @"
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
        private const string SQL_FORMAT_FORCED_DATABASE_CLEANING = @"
                USE [{0}]
                DECLARE @db_name VARCHAR(MAX)
                SET @db_name = '{0}' -- provide your own db name                
                DECLARE @proc_name VARCHAR(MAX)
                DECLARE procedures CURSOR
                FOR
                SELECT   sys.schemas.name + '.' + sys.objects.name
                FROM    sys.objects 
                INNER JOIN sys.schemas ON sys.objects.schema_id = sys.schemas.schema_id
                WHERE sys.objects.[type] = 'P' AND sys.objects.[name] like 'sp_UninstallListenerNotification_%'
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
                WHERE sys.objects.[type] = 'P' AND sys.objects.[name] like 'sp_InstallListenerNotification_%'
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

        private const int COMMAND_TIMEOUT = 60000;

        private static readonly List<int> ActiveEntities = new List<int>();

        private CancellationTokenSource _cts;

        public string ConversationQueueName
        {
            get
            {
                return string.Format("Broker_ListenerQueue_{0}", this.Identity);
            }
        }

        public string ConversationServiceName
        {
            get
            {
                return string.Format("Broker_ListenerService_{0}", this.Identity);
            }
        }

        public string ConversationTriggerName
        {
            get
            {
                return string.Format("Broker_Listener_{0}", this.Identity);
            }
        }

        public string InstallListenerProcedureName
        {
            get
            {
                return string.Format("Broker_InstallListener_{0}", this.Identity);
            }
        }

        public string UninstallListenerProcedureName
        {
            get
            {
                return string.Format("Broker_UninstallListener_{0}", this.Identity);
            }
        }

        private string SelectList = "";

        private static ILogger<SqlDepenecyEx> _logger;

        public string ConnectionString { get; private set; }

        public string DatabaseName { get; private set; }

        public string TableName { get; private set; }

        public string SchemaName { get; private set; }

        public NotificationTypes NotificaionTypes { get; private set; }

        public bool DetailsIncluded { get; private set; }

        private string InsertedPredicate = "";
        private string UpdatedPredicate = "";
        private string DeletedPredicate = "";

        public int Identity
        {
            get;
            private set;
        }

        public bool Active { get; private set; }

        public SqlDependencyEx(
            ILogger<SqlDepenecyEx> logger,
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

        }

        private void SetString()
        {
            var properties = typeof(T).GetProperties();

            if (properties.Any(x => x.GetCustomAttributes(true).Any(y => y as Table != null)))
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

        public void Start(bool forceStopToClean)
        {

            /*
             use this both as a fix for IIS or if you have edited trigger predicate or fields between deployments otherwise can cause message queue to stop picking up messages
             or simply not reflect any changes made
             */

            if (forceStopToClean)
            {
                this.Stop();
            }

            lock (ActiveEntities)
            {
                if (ActiveEntities.Contains(this.Identity))
                    return;
                ActiveEntities.Add(this.Identity);
            }


            _cts = new CancellationTokenSource();
            CancellationToken ct = _cts.Token;

            Task.Factory.StartNew(async () =>
            {
                await this.InstallNotification();
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
                }
            }, _cts.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        public void Stop()
        {
            Task.Factory.StartNew(async () => await UninstallNotification());

            Thread.Sleep(1500);

            lock (ActiveEntities)
                if (ActiveEntities.Contains(Identity)) ActiveEntities.Remove(Identity);

            if ((_cts == null) || (_cts.Token.IsCancellationRequested))
                return;

            if (!_cts.Token.CanBeCanceled)
                return;

            _cts.Cancel();
        }

        public void Dispose()
        {
            Stop();

            //code below emulated a stop without a remove from database
            lock (ActiveEntities)
                if (ActiveEntities.Contains(Identity)) ActiveEntities.Remove(Identity);

            if ((_cts == null) || (_cts.Token.IsCancellationRequested))
                return;

            if (!_cts.Token.CanBeCanceled)
                return;

            _cts.Cancel();
        }

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
                    while (await reader.ReadAsync())
                        result.Add(reader.GetString(0));
            }

            return result.Select(p => int.TryParse(p, out int temp) ? temp : -1).Where(p => p != -1).ToArray();
        }

        public static async Task CleanDatabase(string connectionString, string database)
        {
            await ExecuteNonQuery(string.Format(SQL_FORMAT_FORCED_DATABASE_CLEANING, database), connectionString);
        }

        private static async Task ExecuteNonQuery(string commandText, string connectionString)
        {
            try
            {
                using (SqlConnection conn = new SqlConnection(connectionString))
                using (SqlCommand command = new SqlCommand(commandText, conn))
                {
                    await conn.OpenAsync();
                    command.CommandType = CommandType.Text;
                    await command.ExecuteNonQueryAsync();
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
            }
        }

        private async Task<string> ReceiveEvent(CancellationToken ct)
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
                    if (!await reader.ReadAsync(ct) || await reader.IsDBNullAsync(0, ct))
                        res = string.Empty;
                    else
                        res = reader.GetString(0);
                }
            }

            return res;
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
                    this.TableName,
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

        public event Func<object, TableChangedEventArgs, Task> TableChanged;

        private async Task OnTableChanged(string message)
        {
            Func<object, TableChangedEventArgs, Task> evnt = this.TableChanged;
            if (evnt == null) return;

            await evnt.Invoke(this, new TableChangedEventArgs(message));
        }

        public event Func<object, Task> NotificationProcessStopped;
        private async Task OnNotificationProcessStopped()
        {
            Func<object, Task> evnt = NotificationProcessStopped;
            if (evnt == null) return;

            await evnt.Invoke(this);
        }

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

    public static class FilterHelpers
    {
        public static bool HasChanged(object type)
        {
            return true;
        }

        public static bool Is(NotificationTypes type, object property, object value)
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
            if (expression is UnaryExpression)
            {
                var unaryExpression = (UnaryExpression)expression;
                var right = RecurseThroughExpression(type, unaryExpression.Operand, true);
                return "(" + NodeTypeToString(unaryExpression.NodeType, right == "NULL") + " " + right + ")";
            }
            if (expression is BinaryExpression)
            {
                var binaryExpression = (BinaryExpression)expression;
                var right = RecurseThroughExpression(type, binaryExpression.Right);
                return "(" + RecurseThroughExpression(type, binaryExpression.Left) + " " + NodeTypeToString(binaryExpression.NodeType, right == "NULL") + " " + right + ")";
            }
            if (expression is ConstantExpression)
            {
                var constantExpression = (ConstantExpression)expression;
                return ValueToString(constantExpression.Value, isUnary, quote);
            }
            if (expression is MemberExpression)
            {
                var member = (MemberExpression)expression;

                if (member.Member is PropertyInfo)
                {
                    var property = (PropertyInfo)member.Member;
                    var colName = property.GetCustomAttributes(true).OfType<Column>().FirstOrDefault() != null ? property.GetCustomAttributes(true).OfType<Column>().FirstOrDefault().Name : property.Name;
                    if (isUnary && member.Type == typeof(bool))
                    {
                        return "((SELECT[" + colName + "] FROM " + NotificationTypeToString(type) + ") = 1)";
                    }
                    return "(SELECT[" + colName + "] FROM " + NotificationTypeToString(type) + ")";
                }
                if (member.Member is FieldInfo)
                {
                    var field = (FieldInfo)member.Member;
                    var colName = field.GetCustomAttributes(true).OfType<Column>().FirstOrDefault() != null ? field.GetCustomAttributes(true).OfType<Column>().FirstOrDefault().Name : field.Name;
                    if (isUnary && member.Type == typeof(bool))
                    {
                        return "((SELECT[" + colName + "] FROM " + NotificationTypeToString(type) + ") = 1)";
                    }
                    return "(SELECT[" + colName + "] FROM " + NotificationTypeToString(type) + ")";
                }
                throw new Exception($"Expression does not refer to a property or field: {expression}");
            }
            if (expression is MethodCallExpression)
            {
                var methodCall = (MethodCallExpression)expression;

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
                    if (concated == "")
                    {
                        return ValueToString(false, true, false);
                    }
                    return "(" + RecurseThroughExpression(type, property) + " IN (" + concated.Substring(0, concated.Length - 2) + "))";
                }
                //FilterHelper Methods
                if (methodCall.Method.Name == "HasChanged")
                {
                    if (type != NotificationTypes.Update)
                    {
                        throw new Exception("Method only supported for Update: " + methodCall.Method.Name);
                    }
                    return "(" + RecurseThroughExpression(type, ((UnaryExpression)methodCall.Arguments[0]).Operand) + " <> " + RecurseThroughExpression(NotificationTypes.Delete, ((UnaryExpression)methodCall.Arguments[0]).Operand) + ")";
                }
                if (methodCall.Method.Name == "Is")
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
            if (value is bool)
            {
                if (isUnary)
                {
                    return (bool)value ? "(1=1)" : "(1=0)";
                }
                return (bool)value ? "1" : "0";
            }
            return (quote ? "'" : "") + value.ToString() + (quote ? "'" : "");
        }

        private static bool IsEnumerableType(Type type)
        {
            return type
                .GetInterfaces()
                .Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>));
        }

        private static object GetValue(Expression member)
        {
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