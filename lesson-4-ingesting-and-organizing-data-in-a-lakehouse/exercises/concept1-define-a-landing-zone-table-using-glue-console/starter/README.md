# Glue Tables
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

Let's go over to Athena, and query the customer_landing_zone table.

Enter a simple query like: `select * from customer_landing_zone` and click run.

Now that you see results, you can use any desired SQL query parameters further refine your query and analyze the data in the landing zone.

# Reverse Engineer a Table

Under tables, click the three dots next to `customer_landing`, and click Generate table DDL:
