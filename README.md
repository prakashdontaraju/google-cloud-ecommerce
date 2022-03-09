# 2 Data Pipelines To Easily Know Customer Purchasing Behaviors

Both Steaming Pipeline and Batch Pipeline on Google Cloud

## Business Case

Our customer (company) from the ecommerce space decided to move its data processing, storage and analytics workloads to the Google Cloud Platform as part of their goal to provide their customers (end user) a better experience.

### Results

I successfully engineered streaming & batch data processing pipelines on the Google Cloud Platform.

I created the data pipeline infrastructure on Google Cloud for analyzing customer purchasing behavior in real-time and perfomed the analysis.

### Deployment

I plan to write a blog post about how to deploy these 2 pipelines on Google Cloud soon. Stay tuned!

## Data

I chose the [eCommerce behavior data from multi category store](https://www.kaggle.com/mkechinov/ecommerce-behavior-data-from-multi-category-store) available on Kaggle to focus on successfully implementing streaming and batch pipelines.

I pre-process (transform) data but real business data requires significantly more pre-processing as it's quality may not be ideal for the business problem(s) at hand.

### Properties of data

Data file contains customer behavior data on a large multi-category online store's website for 1 month (November 2019).
 
 Each row in the file represents an event.

* All events are related to products and users

* There are 3 different types of events &#8594; view, cart and purchase

The 2 purchase funnels are
* view &#8594; cart &#8594; purchase
* view &#8594; purchase


## Streaming & Batch Pipelines on Google Cloud

### Implementation

![Streaming & Batch Pipelines on Google Cloud](https://github.com/prakashdontaraju/google-cloud-ecommerce/blob/master/images/google_cloud_pipeline.png)

### Storage

BigQuery (Storing streaming data)

![Streaming Data in BigQuery](https://github.com/prakashdontaraju/google-cloud-ecommerce/blob/master/images/bigquery_store.png)

Cloud Spanner (Storing data in batches)

![Batch Data in Cloud Spanner](https://github.com/prakashdontaraju/google-cloud-ecommerce/blob/master/images/spanner_store.png)

## Analysis

* Daily event count

![Daily Event Count](https://github.com/prakashdontaraju/google-cloud-ecommerce/blob/master/images/daily_events.png)

* Most visited sub-categories

![Most Visited Sub-Categories](https://github.com/prakashdontaraju/google-cloud-ecommerce/blob/master/images/most_visited_subcategories.png)

* Hour vs Event Type vs Price

![Hour vs Event Type vs Price](https://github.com/prakashdontaraju/google-cloud-ecommerce/blob/master/images/hour_event_price.png)

* Purchase conversion volume

![Purchase Conversion Volume](https://github.com/prakashdontaraju/google-cloud-ecommerce/blob/master/images/purchase_conversion_volume.png)

* Purchase conversion rate

![Purchase Conversion Rate](https://github.com/prakashdontaraju/google-cloud-ecommerce/blob/master/images/purchase_conversion_rate.png)

## Connect With Me
**Prakash Dontaraju** [LinkedIn](https://www.linkedin.com/in/prakashdontaraju) [Twitter](https://twitter.com/WittyGrit) [Medium](https://medium.com/@wittygrit)
