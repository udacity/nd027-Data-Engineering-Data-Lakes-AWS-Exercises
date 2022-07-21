# Glue Dynamically Generated Schema

In the previous lesson, we used the Glue Console to define a **glue table **one field and datatype at a time. This can be helpful, especially when the data is known, and is not likely to change. There are other circumstances where the data is not fully defined, or perhaps not fully known. 

<br data-md>

Using dynamically created or updated schemas, you can leverage glue to define your glue tables. This approach also allows a preview of the generated schema, allowing you to define it manually if needed.

<br data-md>

# Glue Studio

Go to Glue Studio, and open the previously created Glue Job

<br data-md>

Search for Glue Studio in the AWS Console search bar

Click the three bars (hamburger menu) on the upper left corner of the AWS Console

Click **Jobs**

Click the job you created previously

You should see the visual data flow

Click the data target (customer trusted zone)

You should see the data target properties tab open

Notice that the option **Do not update the Data Catalog** has been selected

Click **Create a table in the Data Catalog and on subsequent runs, update the schema**

Notice the red alert with the number 1

This means we haven't finished setting up the destination table

Select the database we created earlier

Enter the table name

Click output schema to see the generated schema

Click save