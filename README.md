# ServiceBrokerListener
Cross-platform .NET 4.7.1 (C# 7.0) and .NET Core compatible component which receives SQL Server table changes into your .net code.

Take a look how it works: http://sbl.azurewebsites.net

# How To Use

1. Copy [SqlDependecyEx](https://github.com/AJHopper/ServiceBrokerListener/blob/master/ServiceBrokerListener/ServiceBrokerListener.Domain/SqlDependencyEx.cs) class from `ServiceBrokerListener.Domain` project into your solution.
2. Make sure that Service Broker is enabled for your database.
    
    ```
    ALTER DATABASE test SET ENABLE_BROKER
    
    -- For SQL Express
    ALTER AUTHORIZATION ON DATABASE::test TO userTest
    ```
	
3. Setting up a class for parsing to

When creating a class to pass to, there a couple of things to note:
	1. Only Properties will be mapped to when it parses, fields will not be.
	2. Fields can however be used in the filter, which will allow you to filter on columns not included in the model you recieve back
	3. you can map up enums using the [XMLEnum] standard attribute
	4. A model can map to more than one table, if you intend to do this, all properties will need to be marked using the [Table] attribute to specify what table data is from
	5. A model can have properties with names varying from the table column name, to do this the property will need to be marked with the [Column] attribute
	6. You can use SQL built in commands / fields such as SYSTEM_USER, to do this the field will need to be marked with the [IsSystem] attribute.
	
	Example of two properties within 1 class using different tables:
	```
		[Table("ORD_DETAIL2")]
        [Column("SYSTEM_USER")]
        [IsSystem]
        public string SYSTEM_USER3 { get; set; }
		
		[Table("SOP_CANCELLATION_LINK")]
        [Column("SCL_ORDER_DETAIL_LINK")]
        public int? CancelId { get; set; }
	```
	
4. Batch Message set up

The system also supports the concept of Batch Messaging - this is a special type of handler that runs a stored procedure to check for updates on table, and if conditions are met create a message encompassing the list of all related objects - an example can be seen below

	```
	[ExcludeFromSelect]
        [SqlStuff]
        [DeleteOn]
        public int Id { get; set; }

        [IsSystem]
        [Column("MAX(RunAt)")]
        [ExcludeFromSelect]
        public string RunAt { get; set; }

        [GroupBy]
        public int JobType { get; set; }

        [GroupBy]
        [ExcludeFromSelect]
        public Type ObjectType { get; set; }

        [GroupBy]
        [ExcludeFromSelect]
        public string BatchGroup { get; set; }

        [SqlStuff]
        public string Data { get; set; }
	```
Within this example we are using a separate table solely for batch messaging - as regular messages come in we insert into this batch table to then later group them together.
Above we make use of the "Max(RunAt)" which will determine when the stored procedure processes the batch, the "BatchGroup" to group all related messages together along with the "Id" field to delete the processed messages once they have been handled.

Batch messaging is a way for you to (as an example) have a message come in for each line on an order, put them into a table to wait until all processes impacting lines on the order are complete, and then send an single update with data from all lines that were impacted.
    
5. Settng up and using the class

The intial set up is done using generics to allow for the program to work with any class, the reasons for requiring this is to allow the class to return you the Inserted and Deleted as objects as opposed to as XML, keeping any parsing that may need to be done internally wrapped SqlDependencyEx class
    ```
    // See constructor optional parameters to configure it according to your needs
    var listener = new SqlDependencyEx<T>(ILogger<SqlDependencyEx<T>>,connectionString, "YourDatabase", "YourTable");
	```

After setting up the class you can add a range of filtering - this will add if conditions around trigger generated to cause it to crate messages only when conditions are met
	
	```
	//this can be done using standard lambas, or by also involving the Filterhelper class which is explained in section 5.
	listener.AddTriggerContitions(x=> x.property == "Test", FilterHelpers.HasChanged(x.property));	
	```

Setting up the rest is fairly easy, adding an on table changed event in async and then calling start.
	```
    // e.Data contains actual changed data in the form of two objects of the earlier generic class T
    listener.TableChanged += async (o, e) => Console.WriteLine("Your table was changed!");
    
    // After you call the Start method you will receive table notifications with the actual changed data    
    listener.Start(true);
    
    // ... Your code is here 
    
    // Don't forget to stop the listener somewhere!
    listener.Stop();
    ```
	
6. The FilterHelper class:

The FilterHelper class is a small class with two functions in it, the primary aim is to allow for a little more customisation when filtering.

FilterHelpers.HasChanged(object x) ```//This function should be given a property from your lambda x class, the function will add a condition on the Update for triggers to see if this property has changed```
FilterHelpers.Is(NotificationTypes type, object property, object value)
```
//This Section function is is intended to give more flexibility when checking things, the defaul lambda will always compare the property on the INSERT statement in SQL, this was added to allow you to compare values on DELETE as well
```

	
7. Enjoy!

# How to use for multiple tables

All you need to do is to create multiple listeners with different identities as shown below:

    var listener = new SqlDependencyEx<T>(ILogger<SqlDependencyEx<T>>,connectionString, "YourDatabase", "YourTable", identity: 1);
    var listener2 = new SqlDependencyEx<T>(ILogger<SqlDependencyEx<T>>,connectionString, "YourDatabase", "YourTable2", identity: 2);
    
# How to use in multiple apps

You **must** create listeners with **unique** identities for each app. So, only one listener with a specific identity should exist at the moment. This is made in order to make sure that resources are cleaned up. For more information and best practices see @cdfell [comment](https://github.com/dyatchenko/ServiceBrokerListener/issues/29#issuecomment-241826532) and [this](https://github.com/dyatchenko/ServiceBrokerListener/issues/29#issuecomment-241943608) answer.

Application 1:

    // identities are different
    var listener = new SqlDependencyEx<T>(ILogger<SqlDependencyEx<T>>,connectionString, "YourDatabase", "YourTable", identity: 1);
    
Application 2:

    // identities are different
    var listener2 = new SqlDependencyEx<T>(ILogger<SqlDependencyEx<T>>,connectionString, "YourDatabase", "YourTable2", identity: 2);
    
# How to track UPDATEs only

The `listenerType` constructor parameter configures `SqlDependencyEx` to fire an event for
different notification types (can fire on INSERT, UPDATE, DELETE separately or together)

    var listener = new SqlDependencyEx<T>(ILogger<SqlDependencyEx<T>>,connectionString, "YourDatabase",
                 "YourTable1", listenerType: SqlDependencyEx.NotificationTypes.Update);
    
# Licence

[MIT](LICENSE)
