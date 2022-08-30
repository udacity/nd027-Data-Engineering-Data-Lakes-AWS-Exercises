# Create an Accelerometer Trusted Zone

Now that we have the sensitive data in the **accelerometer landing zone,** we can write a glue job that filters the data and moves compliant records into an** accelerometer trusted zone** for later analysis. Let's go to Glue Studio, and create a new Glue Job:

- Search for Glue Studio in the AWS Console search bar
- Click the three bars (hamburger menu) on the upper left corner of the AWS Console
- Click **Jobs**
- Accept the Visual with a source and target, then click Create
- You should see the **default** visual data flow

# Configure the Job

Define Names for:

* Job (Accelerometer Landing to Trusted)
* Accelerometer Landing Node
* Join Customer Node
* Accelerometer Trusted Node

Define:

* IAM Role
* Job name
* Data Source Data Catalog table (accelerometer_landing)

The Visual Graph should look similar to this (**we're not finished!**):

* Data source type is **Data Catalog **(the accelerometer_landing table we created earlier)
* Transform type defaulted to **ApplyMapping** but the actual transformation will be a **Join**
* Data target defaulted to **S3 **(we will change this to Data Catalog later)

<br data-md>

We need the **Customer Trusted Zone** **S3 datasource**. Click the Source dropdown, click Amazon S3:

* Configure the Amazon S3 datasource to point to the **Customer Trusted Zone** S3 location
* Name the Node (**Customer Trusted Zone)**
* Click the **Infer schema **button

Click the **Output schema** tab and you should see the inferred schema:

As promised, now we change the Transform from **ApplyMapping** to **Join**

Check out our newly created join!

Connect the **Customer Trusted Zone** node.

Click Add Condition.

Choose the join fields you identified earlier to join accelerometer and customer.


Congratulations! You have created a join that will automatically drop Accelerometer rows unless they can be joined to a customer record in the **Trusted Zone:**

# Glue Dynamically Generated Schema

In the previous lesson, we used the Glue Console to define a **glue table **one field and datatype at a time. This can be helpful, especially when the data is known, and is not likely to change. There are other circumstances where the data is not fully defined, and new fields are being introduced. 

Using dynamically created or updated schemas, you can leverage glue to **automatically define your glue tables**. This approach also provides a preview of the generated schema, allowing you to define it manually if needed. We will use this approach in creating our **Accelerometer Trusted Zone** table.

* Click the **Accelerometer Trusted **Node
* Click Data target properties tab
* Add the new S3 path to the Accelerometer Trusted Zone: be sure it ends with a /
* Click **Create a table in the Data Catalog and on subsequent runs, update the schema**

Click output schema to see the generated schema, notice the fields from **both **tables appear

Add another transformation to drop the unwanted fields

Click the Transform drop-down, and choose the **Drop Fields** transformation. Configure the Drop Fields transformation to:

* transform data **after** the Join Customer Node
* remove fields that do not belong to accelerometer
* load data to the Accelerometer S3 Trusted Zone (**we already have this node, just make Drop Fields its parent)**


Click save and then click **Run**.

After the job successfully runs, a new glue table should appear in the Athena query editor.