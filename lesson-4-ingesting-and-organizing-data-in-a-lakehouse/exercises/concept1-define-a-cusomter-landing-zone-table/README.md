# Tackle Boxes and Glue

Have you ever been fishing with an expert fisher? One thing you will notice is that they usually have something called a "Tackle Box." The Tackle Box has all of the bait and lures they can use to catch each variety of fish they are hunting down. 

Looking inside the tackle box, you see a rainbow of colors, and shapes and sizes. This tackle box represents all the types of fish that can be caught. If they wanted to catch more types of fish, your fishing friend would add more lures or bait to their tackle box. 

A Glue Catalog represents many sources and destinations for data. They can represent Kafka, Kinesis, Redshift, S3, and many more. If we want to connect to another source of data, we need to add it to the catalog. This makes querying data much easier because just like catching a fish, we know where to look for the type of data we are looking for.

# Glue Tables

A **glue table** is a definition of a specific group of fields, that represent a logical entity. The Glue Catalog is made up of multiple table definitions. These tables are not physically stored in Glue. Glue is just the catalog layer. They store a reference to the data we can query or store. 

<br data-md>

There are three main ways to define a glue table in the glue catalog:

* use Glue Console to define each field
* configure a Glue Job to automatically generate a table definition
* use SQL to define a table with DDL (Data Definition Language) or create statements

# Using the Glue Console to Define a Table

Imagine you have the Customer data we looked at earlier in an S3 bucket directory, and you want to get an idea of how many records have been placed in the Customer Landing Zone. You could create a **glue table** definition to query the data using SQL.

<br data-md>

Let's go over to the Glue Catalog in the Glue Console. Search for Glue Catalog in the AWS Console, and you will see the Glue Data Catalog. Click Data Catalog.

# Create a Database

Click the Add database button.

Enter the name of your database.

On the menu, click tables.

Select add table manually.

Enter the name of the table you are defining, and the database it belongs in, and click next.

# Add the physical data store

Choose the folder icon to drill down to the directory that contains the landing zone data.

# Choose the data format

Choose JSON for the data format for your customer landing zone data, and click next.

# Define the fields

Look at the sample JSON data below:

```
{
"customerName":"Frank Doshi",
"email":"Frank.Doshi@test.com",
"phone":"8015551212",
"birthDay":"1965-01-01",
"serialNumber":"159a908a-371e-40c1-ba92-dcdea483a6a2",
"registrationDate":1655293787680,
"lastUpdateDate":1655293787680,
"shareWithResearchAsOfDate":1655293787680,
"shareWithPublicAsOfDate":1655293787680
}
```

<br data-md>

Using a sample record, define the fields in the glue table, and click next.

<br data-md>

# Partition Indices

We can partition a table based on the index, or field, so that data is separated by key fields. For now we are going to skip this, click next, and then finish to confirm the table.

# Athena - a Glue Catalog Query Tool

Now that you have defined a table using the glue catalog, you might want to query the table. Previously we had to use Spark SQL and relied on Spark schemas to query data. Using the glue data catalog, we can query data using Athena.

Let's go over to Athena, and query the customer_landing table.

Athena uses S3 to store query results. Set up the location Athena will use from now going forward.

Click the View Settings button.

Enter the full S3 path you want Athena to save query results. Encryption makes it less likely that sensitive data will be compromised. For this exercise we will skip encryption.

Enter a simple query like: `select * from customer_landing` and click run.

Now that you see results, you can use any desired SQL query parameters further refine your query and analyze the data in the landing zone.

# Reverse Engineer a Table

Sometimes, it is helpful to pass on the schema definition in git, to other co-workers, for example. The easiest way to pass on a schema is through DDL (Data Definition Language) SQL statements. Now that you've generated a table in glue, you can reverse engineer it, or generate the SQL statements.

Under tables, click the three dots next to `customer_landing`, and click Generate table DDL.

Save the script as `customer_landing.sql`.

Repeat the steps above for the `customer_trusted` table. 

**Hint: Choose Parquet for the format**
