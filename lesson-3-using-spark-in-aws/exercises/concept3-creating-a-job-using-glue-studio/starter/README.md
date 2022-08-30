# Create a Spark Job with Glue Studio

To use Glue Studio, search for it in the AWS Console. Then click the **AWS Glue Studio** menu option.

Select **Jobs** from the Glue Studio Menu

To get started, go with the default selection - **Visual with a source and a target,** and click **Create**

Before we forget, under **Job Details **create a **name** for the Job, and choose the **IAM Role **the job will use during execution. This should be the Glue Service Role you created earlier.

# Ingest Customer Data

Let's assume a website creates a daily JSON file  of all new customers created during the previous 24 hours. That JSON file will go into the S3 **landing zone** designated for new data. A landing zone is a place where new data arrives prior to processing.

We can copy a sample customer file into S3 using the the AWS Command Line Interface (CLI). In the command below the blanks should be replaced with (1) the file name you want to copy to S3, and (2) the name of the S3 bucket you created.

***Note when specifying a directory in S3, always include a trailing slash `/` at the end of the directory name. If you forget the trailing slash `/`, it will be confusing, because S3 won't respond appropriately***

<br data-md>

`aws s3 cp ./project/starter/customer/_______ s3://_______/customer/landing/`

<br data-md>

# Privacy Filter

One of the most important transformations is excluding Personally Identifiable Information (PII). Glue has an out-of-the-box filter, but we are going to make our own. For the **source** in your Glue Studio Job, choose the S3 bucket you created earlier, with the folder containing the **raw** or **landing** customer data. The folder should have a forward-slash / on the end. 

*Under Node properties, name the Data source appropriately, for example: Customer Landing or Customer Raw.*

For the **transformation**, select the **filter** option

<br data-md>

Filter on the  *shareWithResearchAsOfDate* timestamp field, and configure it to eliminate any customers that have a non-zero *shareWithResearchAsOfDate*.

*Name the Transform appropriately, for example: Share With Research *

For your destination choose an S3 location for customers who have chosen to share with research, or a **trusted zone**. The S3 bucket should be the bucket you created earlier. Any time you specify a folder that doesn't exist, S3 will automatically create it the first time data is placed there. Be sure to add the forward-slash on the end of the folder path.

# Save and Run the Job

You will notice the red triangle at the top of the screen saying "Job has not been saved." Click the Save button, then click Run:

On the green ribbon, click the** Run Details** link

You will then see the run details. By default the job will run three times before quitting. To view the logs, click the **Error Logs** link. This includes all of the non-error logs as well.

To see the logs in real-time, click the **log stream id**

If the log viewer stops, click the **resume** button. It often pauses if the output  takes more than a couple of seconds to progress to the next step. Notice that most of the output in this example is INFO. If you see a row marked as ERROR, you can expand that row to determine the error.

# View the Generated Script

To view the generated script, go back to Glue Studio, and click the **Script** tab: