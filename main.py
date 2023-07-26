# google cloud bigquery api requires Python >= 3.7

# [Independency]
import pandas as pd
import numpy as np
import pycaret
from pycaret.classification import load_model,predict_model,set_config
import sklearn
import datetime
import os
from google.cloud import storage
from google.cloud.storage import Blob
from google.cloud import bigquery


if __name__ == "__main__":

    # [environmental variables]
    project = 'my_project_id'
    # test dataset in test account
    dataset_id = "my_dataset"
    # bigquery api
    client = bigquery.Client()
    input_table_id = 'my_bigquery_input_data_table'
    output_bucket_id = 'my_cloud_bucket_for_output'


    # [data processing functions]
    def conditions(df):
        if ((df['first_camera_time'] is not pd.NaT) and (df['first_camera_time'] <= df['first_corners_time'] or (df['first_corners_time'] is pd.NaT)) and (df['first_camera_time'] <= df['first_square_time'] or (df['first_square_time'] is pd.NaT))) :
            return 'camera'
        elif ((df['first_square_time'] is not pd.NaT) and (df['first_square_time'] <= df['first_corners_time'] or (df['first_corners_time'] is pd.NaT)) and (df['first_square_time'] <= df['first_camera_time'] or (df['first_camera_time'] is pd.NaT) )):
            return 'square'
        elif ((df['first_corners_time'] is not pd.NaT) and (df['first_corners_time'] <= df['first_camera_time'] or (df['first_camera_time'] is pd.NaT)) and (df['first_corners_time'] <= df['first_square_time']  or (df['first_square_time'] is pd.NaT))):
            return 'corners'
        else:
            return 'no_creation'

    def preprocess(df_raw):
        df = df_raw.copy()
        df['application_opened'] = df['application_opened']+1
        
        df['sign_up_time'] = pd.to_datetime(df['sign_up_time'], errors='coerce')
        df['first_project_time'] = pd.to_datetime(df['first_project_time'], errors='coerce')
        df['first_room_time'] = pd.to_datetime(df['first_room_time'], errors='coerce')
        df['first_camera_time'] = pd.to_datetime(df['first_camera_time'], errors='coerce')
        df['first_square_time'] = pd.to_datetime(df['first_square_time'], errors='coerce')
        df['first_corners_time'] = pd.to_datetime(df['first_corners_time'], errors='coerce')
        df['first_export_time'] = pd.to_datetime(df['first_export_time'], errors='coerce')
        df['first_marketing_values_added_time'] = pd.to_datetime(df['first_marketing_values_added_time'], errors='coerce')
        df['first_purchase_screen_time'] = pd.to_datetime(df['first_purchase_screen_time'], errors='coerce')        
        
        # calculate the difference between that action timestamp and sign_up timestamp
        # create aggreagated time features: (minutes)
        agg_names = ['first_project_time_diff','first_room_time_diff','first_camera_time_diff','first_square_time_diff','first_corners_time_diff','first_export_time_diff','first_marketing_values_added_time_diff','first_purchase_screen_time_diff']
        
        cols = ['first_project_time','first_room_time','first_camera_time','first_square_time','first_corners_time','first_export_time','first_marketing_values_added_time','first_purchase_screen_time']
        
        for i in range (len(agg_names)):
            df[agg_names[i]] = (df[cols[i]] - df['sign_up_time']).dt.total_seconds() / 60
            df.loc[df[agg_names[i]] < 0, agg_names[i]] = np.nan

        # create another feature: 
        # conditions defined in another function

        df['agg_first_room_create_method'] = df.apply(conditions, axis=1)
        
        # create another feature:
        # get the time difference between first_project_time and first_room_time  (minutes)
        # to know how long it takes the user to create a room after it figures out how to create a project 
        df['diff_first_project_room'] = (df['first_room_time'] - df['first_project_time']).dt.total_seconds()/ 60

        df.loc[df['diff_first_project_room'] < 0, 'diff_first_project_room'] = np.nan

    # drop unnessary columns
        drop_features= ['sign_up_os','sign_up_time','first_project_time','first_room_time','first_camera_time','first_square_time','first_corners_time','first_export_time','first_marketing_values_added_time','first_purchase_screen_time']
        
        df.drop(columns=drop_features , inplace = True)
        
        return df


    # [function to upload csv file to bucket]
    def upload(csvfile, bucketname, blobname):
        client = storage.Client()
        bucket = client.get_bucket(bucketname)
        blob = Blob(blobname, bucket)
        blob.upload_from_filename(csvfile)
        gcslocation = 'gs://{}/{}'.format(bucketname, blobname)
        return gcslocation




    # [load raw data from bigquery]
    # Get bigquery data table to DataFrame

    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    #test table name 
    table_ref = dataset_ref.table(input_table_id)
    table = client.get_table(table_ref)
    raw_data = client.list_rows(table).to_dataframe()




    # [load model]
    # pkl file is generated from pycaret package 
    model = load_model('model-pkl-file')

    # call preprocessing function
    test_data = preprocess(raw_data)

    # fix the error caused by null values
    #test_data.replace({pd.NA: np.nan})
    #test_data.replace(to_replace=pd.NA, value=None, inplace=True)
    
    test_data.replace(to_replace=pd.NA, value=np.nan, inplace=True)

    datatype= {'user_id': object, 'country': object, 'marketing_use_case':object, 'marketing_intent':object, 'industry_claim':'int64', 'industry_other':'int64', 'industry_reno':'int64', 'industry_surveys':'int64', 'marketing_team':object, 'first_device_model':object, 'first_device_type':object, 'first_device_lidar':object, 'first_photo_add_time':object, 'first_project_publishing_succeeded':object, 'first_three_d_view_clicked':object, 'first_floor_creation_succeeded':object, 'first_project_sharing_succeeded':object, 'first_activation_screen_time':object, 'application_opened':'int64', 'application_updated':'int64', 'room_creation_succeeded':'int64', 'room_method_corners':'int64', 'room_method_camera':'int64', 'room_method_square':'int64', 'project_export_succeeded':'int64', 'marketing_values_added':'int64', 'screen_name_activation':'int64', 'screen_name_purchase':'int64', 'demo_project_clicked':'int64', 'demo_project_opened':'int64', 'first_project_time_diff':'float64', 'first_room_time_diff':'float64', 'first_camera_time_diff':'float64', 'first_square_time_diff':'float64', 'first_corners_time_diff':'float64', 'first_export_time_diff':'float64', 'first_marketing_values_added_time_diff':'float64', 'first_purchase_screen_time_diff':'float64', 'agg_first_room_create_method':object, 'diff_first_project_room':'float64'}

    # fix the unexpected bool type error 
    test_data.astype(datatype).dtypes



    # prediction_df is a pandas dataframe with label and scores

    prediction_df = predict_model(model,data=test_data,raw_score=True,verbose=True)
    prediction_df_selected = prediction_df[['user_id','Label','Score_0','Score_1']]
    
    # add a new column of the data to help for further query in bigquery
    d=(datetime.datetime.now()).strftime('%Y-%m-%d')
    prediction_df_selected["prediction_date"]=d

    # [load prediction df as csv and upload to cloud storage bucket]
    csvfile = 'user_purchase_likelihood_{}.csv'.format((datetime.datetime.now()).strftime('%Y-%m-%d'))
    bucket = output_bucket_id

    prediction_df_selected.to_csv(csvfile, index=False) 
    #test_data.to_csv(csvfile,index=False)
    
    gscloc = upload(csvfile,bucket,format(os.path.basename(csvfile)))