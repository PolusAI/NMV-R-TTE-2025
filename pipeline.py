######## GLOBAL CODE
# We can add this code to the GLOBAL CODE on the RHS pane
# Import the data types we will be using to functions and schemas
from pyspark.sql.types import StructType, DateType, StructField, StringType, IntegerType, FloatType, DoubleType
from pyspark.sql import Row

# Import functions and tools to make functions, and interact with columns with dot functions or SQL functions
from pyspark.sql.functions import lower, upper, col, udf, monotonically_increasing_id, to_date, trim, ltrim, rtrim, avg
from pyspark.sql.functions import length, size, unix_timestamp, from_unixtime, broadcast, to_timestamp, split, when, rand, count, round, countDistinct, product

# Additional Functions
from pyspark.sql.functions import min, max, col, mean, lit, sum, when, regexp_replace, lower, upper, concat_ws, to_date, floor, months_between, datediff, date_add, current_date, least, greatest, last_day, last, expr

# Functions for window functions
from pyspark.sql import Window
from pyspark.sql.functions import row_number

### PYTHON Functions
import datetime as dt

## GLOBAL PY CODE
### Import all necessary packages
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re

# Pandas functions
idx = pd.IndexSlice

# Viewing related functions
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


@transform_pandas(
    Output(rid="ri.foundry.main.dataset.962db171-49d5-43cb-b07c-70bb82cfcda1"),
    DESCRIPTIVES_POST_MATCHING=Input(rid="ri.foundry.main.dataset.15f1209c-da22-4c27-a767-6611f088a673"),
    DESCRIPTIVES_PRE_MATCHING=Input(rid="ri.foundry.main.dataset.cfe6929b-fb38-4400-b787-0ced00b2a65b"),
    testing_balance_copied_1=Input(rid="ri.foundry.main.dataset.3ce9c84e-3810-4a14-93c3-b73000249aa8")
)
def DESCRIPTIVES_MATCHING_PLUS_SMD_MERGED(DESCRIPTIVES_POST_MATCHING, DESCRIPTIVES_PRE_MATCHING, testing_balance_copied_1):
    DESCRIPTIVES_POST_MATCHING = DESCRIPTIVES_POST_MATCHING
    testing_balance = testing_balance_copied_1
    DESCRIPTIVES_PRE_MATCHING = DESCRIPTIVES_PRE_MATCHING

    # This dataset will merge the pre and post matching descriptives, and the SMDs from balance test

    pre_match = DESCRIPTIVES_PRE_MATCHING
    post_match = DESCRIPTIVES_POST_MATCHING.drop(['scale','rank'], axis= 1)
    smds = testing_balance

    # Before merging pre and post, we should rename the columns
    columns_to_rename = ["Control_N_or_M","Control_Std_OR_Total","Treatment_N_or_M","Treatment_Std_OR_Total"]
    new_column_names_prematch = ["Before_Matching_Control_N_or_M","Before_Matching_Control_Std_OR_Total","Before_Matching_Treatment_N_or_M","Before_Matching_Treatment_Std_OR_Total"]
    new_column_names_postmatch = ["After_Matching_Control_N_or_M","After_Matching_Control_Std_OR_Total","After_Matching_Treatment_N_or_M","After_Matching_Treatment_Std_OR_Total"]
    prematch_rename_dict = dict(zip(columns_to_rename, new_column_names_prematch))
    postmatch_rename_dict = dict(zip(columns_to_rename, new_column_names_postmatch))
    pre_match = pre_match.rename(columns = prematch_rename_dict)
    post_match = post_match.rename(columns = postmatch_rename_dict)

    # Merge and then sort by rank
    final = pd.merge(left = pre_match, right = post_match, on = 'variable', how = 'inner')
    final = pd.merge(left = final, right = smds, on = 'variable', how = 'left')

    # Return final
    return final.sort_values(by = 'rank')
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.15f1209c-da22-4c27-a767-6611f088a673"),
    DESCRIPTIVES_PRE_MATCHING=Input(rid="ri.foundry.main.dataset.cfe6929b-fb38-4400-b787-0ced00b2a65b"),
    eligible_sample_EHR_all=Input(rid="ri.foundry.main.dataset.bb3f1155-74ee-4013-b621-67e08eca5aeb"),
    nearest_neighbor_matching_all=Input(rid="ri.foundry.main.dataset.7ed3e283-f6c6-43f0-baf6-ee29ea81c982"),
    testing_balance_copied_1=Input(rid="ri.foundry.main.dataset.3ce9c84e-3810-4a14-93c3-b73000249aa8")
)
def DESCRIPTIVES_POST_MATCHING(DESCRIPTIVES_PRE_MATCHING, nearest_neighbor_matching_all, eligible_sample_EHR_all, testing_balance_copied_1):
    DESCRIPTIVES_PRE_MATCHING = DESCRIPTIVES_PRE_MATCHING

    # Start with the pre-matching dataset. This should also contain the target variables
    pre_matching_cohort = eligible_sample_EHR_all
    matching_data = nearest_neighbor_matching_all

    # Join the pre_matching_cohort to the matching output to retain matched pairs
    pre_matching_cohort = pre_matching_cohort.join(matching_data.select('person_id','subclass').where(expr('subclass IS NOT NULL')), on = 'person_id', how = 'inner')
    df = pre_matching_cohort

    # Set up the 
    id_variable = 'person_id'
    treatment = 'paxlovid_treatment'
    target_variable = 'composite60'
    
    # Set up column lists: demographics, BMI, vaccination, covariates (drugs and procedures), and optionally - other treatments
    treatment_exposure_columns = ["montelukast_treatment","fluvoxamine_treatment","ivermectin_treatment"]

    demographics = [
        'female',
        "race_ethnicity_nhpi",
        "race_ethnicity_white",
        "race_ethnicity_hispanic_latino",
        "race_ethnicity_aian",
        "race_ethnicity_black",
        "race_ethnicity_asian",
        # 'age_at_covid',
    ] + [column for column in df.columns if 'age_at_covid' in column]

    # bmi_columns = [
    #     "UNDERWEIGHT_LVCF",
    #     "NORMAL_LVCF",
    #     "OVERWEIGHT_LVCF",
    #     "OBESE_LVCF",
    #     "OBESE_CLASS_3_LVCF",
    #     "MISSING_BMI_LVCF",
    # ]

    bmi_columns = []

    covariate_columns = [column for column in pre_matching_cohort.columns if (('covariate_' in column) | ('condition' in column) | ('procedure' in column) | ('vaccinated' in column) | ('risk_factor' in column)) & ('LVCF' in column) & ('exclusion' not in column)]
    print('covariates', covariate_columns)

    # Set up the final list of descriptives columns; Filter the data frame to those columns, and convert to pandas
    descriptives_columns = [treatment, target_variable] + treatment_exposure_columns + demographics + bmi_columns + covariate_columns
    descriptives_columns = [column for column in descriptives_columns if column in df.columns]
    df = df.select(descriptives_columns).toPandas()
    
    # Convert the colum list into a data frame, which we will use to rank order the variables so we can easily merge to labels and sort
    descriptives_columns_enumerated = list(enumerate(descriptives_columns))
    multi_index = pd.MultiIndex.from_tuples(descriptives_columns_enumerated, names = ['rank', 'variable'])
    column_list_dataframe = pd.DataFrame(index = multi_index).reset_index()

    # Convert any remaining char (object) columns to numeric
    char_columns = [column for column in df.columns if column not in [id_variable, treatment, target_variable]]
    char_columns = list(df.select_dtypes('O').columns)
    for column in char_columns:
        df[column] = pd.to_numeric(df[column], errors = 'coerce')

    # Continuous Columns - identify continuous columns; we will take M and SD for these; 
    # binary_columns = list(df.columns[(df.max() == 1).values])
    binary_columns = list(df.columns[[df[column].nunique() <= 2 for column in df.columns]])
    continuous_columns = list(df.columns[(df.max() > 0.01).values])
    continuous_columns = [column for column in continuous_columns if column not in binary_columns]
    print((list(continuous_columns)))

    # For the binary columns perform counts and get percentages
    binary_descriptives = df.groupby(treatment)[binary_columns].agg(['sum','count'])
    # binary_descriptives.columns = [a + '_' + b for a, b in binary_descriptives.columns]

    # Transpose the Data Frame - Make column for Control and Treated
    binary_descriptives = binary_descriptives.T
    binary_descriptives.columns = sorted(binary_descriptives.columns)
    binary_descriptives.columns = ['Control', 'Treated']
    binary_descriptives = binary_descriptives.unstack() # Unstack the dataframe 
    binary_descriptives.columns = [a + '_' + b for a,b in binary_descriptives.columns]

    # Reset the Index
    binary_descriptives = binary_descriptives.rename_axis('variable').reset_index()
    binary_descriptives.columns = ['variable','Control_N_or_M','Control_Std_OR_Total','Treatment_N_or_M','Treatment_Std_OR_Total',]

    # Include a percentage column
    binary_descriptives['Control_N_or_M'] = np.where(binary_descriptives['variable'] == treatment, binary_descriptives['Control_Std_OR_Total'], binary_descriptives['Control_N_or_M'])
    binary_descriptives['Control_Std_OR_Total'] =  binary_descriptives['Control_N_or_M']/binary_descriptives['Control_Std_OR_Total']
    binary_descriptives['Treatment_Std_OR_Total'] =  binary_descriptives['Treatment_N_or_M']/binary_descriptives['Treatment_Std_OR_Total']

    # Repeat for the Continuous Columns
    # For the continuous columns perform counts and get percentages
    continuous_descriptives = df.groupby(treatment)[continuous_columns].agg(['mean','std'])
    # continuous_descriptives.columns = [a + '_' + b for a,b in continuous_descriptives.columns]

    # Transpose the Data Frame - Make separate column for Control and Treated
    continuous_descriptives = continuous_descriptives.T
    continuous_descriptives.columns = sorted(continuous_descriptives.columns)
    continuous_descriptives.columns = ['Control', 'Treated']
    continuous_descriptives = continuous_descriptives.unstack() # Unstack the dataframe
    continuous_descriptives.columns = [a + '_' + b for a,b in continuous_descriptives.columns]

    # Reset the Index
    continuous_descriptives = continuous_descriptives.rename_axis('variable').reset_index()
    continuous_descriptives.columns = ['variable','Control_N_or_M','Control_Std_OR_Total','Treatment_N_or_M','Treatment_Std_OR_Total',]
    continuous_descriptives['scale'] = 'continuous'
    binary_descriptives['scale'] = 'categorical'

    # Concatenate the datasets (UNION)
    combined = pd.concat([binary_descriptives, continuous_descriptives])

    # Merge to the column ranking list
    combined_final = pd.merge(left = combined, right = column_list_dataframe, on = 'variable', how = 'inner')

    # Sort the final list of columns
    combined_final = combined_final.sort_values(by = 'rank')

    return combined_final
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.cfe6929b-fb38-4400-b787-0ced00b2a65b"),
    eligible_sample_EHR_all=Input(rid="ri.foundry.main.dataset.bb3f1155-74ee-4013-b621-67e08eca5aeb"),
    nearest_neighbor_matching_all=Input(rid="ri.foundry.main.dataset.7ed3e283-f6c6-43f0-baf6-ee29ea81c982")
)
def DESCRIPTIVES_PRE_MATCHING(eligible_sample_EHR_all, nearest_neighbor_matching_all):

    # This node computes descriptive statistics for the covariates and demographic variables for the analysis sample prior to matching

    
    # Input the pre-matching dataset. This should also contain the target variables
    pre_matching_cohort = eligible_sample_EHR_all
    df = pre_matching_cohort

    # Set up the 
    id_variable = 'person_id'
    treatment = 'paxlovid_treatment'
    target_variable = 'composite60'
    
    # Set up column lists: demographics, BMI, vaccination, covariates (drugs and procedures), and optionally - other treatments
    treatment_exposure_columns = ["montelukast_treatment","fluvoxamine_treatment","ivermectin_treatment"]

    demographics = [
        'female',
        "race_ethnicity_nhpi",
        "race_ethnicity_white",
        "race_ethnicity_hispanic_latino",
        "race_ethnicity_aian",
        "race_ethnicity_black",
        "race_ethnicity_asian",
        # 'age_at_covid',
    ] + [column for column in df.columns if 'age_at_covid' in column]

    # bmi_columns = [
    #     "UNDERWEIGHT_LVCF",
    #     "NORMAL_LVCF",
    #     "OVERWEIGHT_LVCF",
    #     "OBESE_LVCF",
    #     "OBESE_CLASS_3_LVCF",
    #     "MISSING_BMI_LVCF",
    # ]

    bmi_columns = []
    
    covariate_columns = [column for column in df.columns if (('covariate_' in column) | ('condition' in column) | ('procedure' in column) | ('vaccinated' in column) | ('risk_factor' in column)) & ('LVCF' in column) & ('exclusion' not in column)]
    print('covariates', covariate_columns)

    # Set up the final list of descriptives columns; Filter the data frame to those columns, and convert to pandas
    descriptives_columns = [treatment, target_variable] + treatment_exposure_columns + demographics + bmi_columns + covariate_columns
    descriptives_columns = [column for column in descriptives_columns if column in df.columns]
    df = df.select(descriptives_columns).toPandas()
    
    # Convert the colum list into a data frame, which we will use to rank order the variables so we can easily merge to labels and sort
    descriptives_columns_enumerated = list(enumerate(descriptives_columns))
    multi_index = pd.MultiIndex.from_tuples(descriptives_columns_enumerated, names = ['rank', 'variable'])
    column_list_dataframe = pd.DataFrame(index = multi_index).reset_index()

    # Convert any remaining char (object) columns to numeric
    char_columns = [column for column in df.columns if column not in [id_variable, treatment, target_variable]]
    char_columns = list(df.select_dtypes('O').columns)
    for column in char_columns:
        df[column] = pd.to_numeric(df[column], errors = 'coerce')

    # Continuous Columns - identify continuous columns; we will take M and SD for these; 
    # binary_columns = list(df.columns[(df.max() == 1).values])
    binary_columns = list(df.columns[[df[column].nunique() <= 2 for column in df.columns]])
    continuous_columns = list(df.columns[(df.max() > 0.01).values])
    continuous_columns = [column for column in continuous_columns if column not in binary_columns]
    print((list(continuous_columns)))

    # For the binary columns perform counts and get percentages
    binary_descriptives = df.groupby(treatment)[binary_columns].agg(['sum','count'])
    # binary_descriptives.columns = [a + '_' + b for a, b in binary_descriptives.columns]

    # Transpose the Data Frame - Make column for Control and Treated
    binary_descriptives = binary_descriptives.T
    binary_descriptives.columns = sorted(binary_descriptives.columns)
    binary_descriptives.columns = ['Control', 'Treated']
    binary_descriptives = binary_descriptives.unstack() # Unstack the dataframe 
    binary_descriptives.columns = [a + '_' + b for a,b in binary_descriptives.columns]

    # Reset the Index
    binary_descriptives = binary_descriptives.rename_axis('variable').reset_index()
    binary_descriptives.columns = ['variable','Control_N_or_M','Control_Std_OR_Total','Treatment_N_or_M','Treatment_Std_OR_Total',]

    # Include a percentage column
    binary_descriptives['Control_N_or_M'] = np.where(binary_descriptives['variable'] == treatment, binary_descriptives['Control_Std_OR_Total'], binary_descriptives['Control_N_or_M'])
    binary_descriptives['Control_Std_OR_Total'] =  binary_descriptives['Control_N_or_M']/binary_descriptives['Control_Std_OR_Total']
    binary_descriptives['Treatment_Std_OR_Total'] =  binary_descriptives['Treatment_N_or_M']/binary_descriptives['Treatment_Std_OR_Total']

    # Repeat for the Continuous Columns
    # For the continuous columns perform counts and get percentages
    continuous_descriptives = df.groupby(treatment)[continuous_columns].agg(['mean','std'])
    # continuous_descriptives.columns = [a + '_' + b for a,b in continuous_descriptives.columns]

    # Transpose the Data Frame - Make separate column for Control and Treated
    continuous_descriptives = continuous_descriptives.T
    continuous_descriptives.columns = sorted(continuous_descriptives.columns)
    continuous_descriptives.columns = ['Control', 'Treated']
    continuous_descriptives = continuous_descriptives.unstack() # Unstack the dataframe
    continuous_descriptives.columns = [a + '_' + b for a,b in continuous_descriptives.columns]

    # Reset the Index
    continuous_descriptives = continuous_descriptives.rename_axis('variable').reset_index()
    continuous_descriptives.columns = ['variable','Control_N_or_M','Control_Std_OR_Total','Treatment_N_or_M','Treatment_Std_OR_Total',]
    continuous_descriptives['scale'] = 'continuous'
    binary_descriptives['scale'] = 'categorical'

    # Concatenate the datasets (UNION)
    combined = pd.concat([binary_descriptives, continuous_descriptives])

    # Merge to the column ranking list
    combined_final = pd.merge(left = combined, right = column_list_dataframe, on = 'variable', how = 'inner')

    # Sort the final list of columns
    combined_final = combined_final.sort_values(by = 'rank')

    return combined_final
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.0ef602f8-450d-43f1-a28f-b0dc7947143a"),
    hospitalizations=Input(rid="ri.foundry.main.dataset.65ffc6be-73f1-431d-b720-29da7cbcc433"),
    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.5af2c604-51e0-4afa-b1ae-1e5fa2f4b905"),
    minimal_patient_cohort=Input(rid="ri.foundry.main.dataset.9b0b693d-1e29-4eeb-aa02-a86f1c7c7fb6")
)
def ED_visits(microvisits_to_macrovisits, minimal_patient_cohort, hospitalizations):
    minimal_patient_cohort = minimal_patient_cohort

    # This code will create a data frame of all the EDvisits the patient has experienced
    # For the outcome, we will need to exclude patients hospitalized 30 days before the trial date
    # AND we will need to exclude patients who are hospitalized within 24 hours of entering the trial

    EDvisits = microvisits_to_macrovisits.selectExpr('person_id','visit_start_date AS date','macrovisit_start_date','visit_concept_name').where(expr("visit_concept_name IN ('Emergency Room and Inpatient Visit', 'Emergency Room - Hospital', 'Emergency Room Visit', 'Emergency Room Critical Care Facility')")).withColumn('EDvisit', lit(1)).join(minimal_patient_cohort.select('person_id','COVID_first_poslab_or_diagnosis_date').distinct(), on = 'person_id', how = 'inner').orderBy('person_id','date')

    # We should end up with 1 row per patient with the following columns: FIRST EDvisit on/after COVID-19; MOST RECENT EDvisit date 30 days before COVID-19
    result = EDvisits.withColumn('pre_post_covid', expr('CASE WHEN date < COVID_first_poslab_or_diagnosis_date THEN 1 WHEN date >= COVID_first_poslab_or_diagnosis_date THEN 2 ELSE NULL END'))
    result = result.withColumn('EDvisit_date_post_covid', expr('MIN(CASE WHEN pre_post_covid = 2 THEN date ELSE NULL END) OVER(PARTITION BY person_id)'))
    result = result.withColumn('EDvisit_date_30_days_pre_covid', expr('MAX(CASE WHEN date BETWEEN DATE_ADD(COVID_first_poslab_or_diagnosis_date, -31) AND DATE_ADD(COVID_first_poslab_or_diagnosis_date, -1) THEN date ELSE NULL END) OVER(PARTITION BY person_id)'))
    result = result.withColumn('earliest_EDvisit_date', expr('MIN(date) OVER(PARTITION BY person_id)'))
    result = result.withColumn('EDvisit_date_pre_covid', expr('MAX(CASE WHEN pre_post_covid = 1 THEN date ELSE NULL END) OVER(PARTITION BY person_id)'))

    return result.select('person_id','EDvisit_date_post_covid','EDvisit_date_pre_covid').distinct().where(expr('NOT(EDvisit_date_post_covid IS NULL AND EDvisit_date_pre_covid IS NULL)'))

@transform_pandas(
    Output(rid="ri.vector.main.execute.764fa4d6-054d-44fe-890f-9d7ab433c437"),
    paxlovid_expanded_data_long_covid=Input(rid="ri.foundry.main.dataset.3d2b0e13-8f89-43c2-b72a-f1e6cad12d67")
)
def Get_KM_Curve_month_DTSA(paxlovid_expanded_data_long_covid):

    title = 'Risk of Long COVID'
    inset_title = 'By 180 days'
    time = 'time'
    outcome = 'outcome'
    x_axis_label = 'Month'
    control_group_label = 'Untreated'
    treatment_group_label = 'Treated'
    max_month = 7

    df = paxlovid_expanded_data_long_covid.withColumn('maxtime', expr('MAX(time) OVER(PARTITION BY person_id)')).where(expr('time = maxtime')).drop('maxtime')
    df = df.select('person_id','treatment',time,outcome).toPandas()
    
    #################
    treatment = 'treatment'

    #### STEP 1 - GET THE LIFETABLE - N AT RISK
    from lifelines.utils import survival_table_from_events
    from lifelines import NelsonAalenFitter, KaplanMeierFitter
    from matplotlib.gridspec import GridSpec
    import matplotlib.table as tbl
    from lifelines.plotting import add_at_risk_counts
    
    fig, ax = plt.subplots(1,1, figsize = (6,5))
    set_output_image_type('svg')

    ###### 1. PLOT FOR EARLY TREATMENT ("person_id","weight","treatment","time","outcome")
    # First plot no treatment
    f1 = KaplanMeierFitter()
    f1.fit(durations = df.loc[(df[treatment] == 0), time], 
    event_observed = df.loc[(df[treatment] == 0), outcome],
    label = control_group_label, 
    # weights = df.loc[(df[treatment] == 0), 'weight'], 
    )

    # Plot the cumulative density for 1 group
    f1.plot_cumulative_density(ax = ax, ci_show = True)

    

    ############### Get lifetable
    table1 = survival_table_from_events(death_times = df.loc[(df[treatment] == 0), time], 
                                    event_observed = df.loc[(df[treatment] == 0), outcome],
    #                                    intervals = 4
                                        )

    ### GET CUMULATIVE EVENTS AND PERCENT DIED
    final_events1 = pd.DataFrame(table1.agg({'at_risk': lambda x: x.iloc[1], 'observed':np.sum})).T
    final_events1['percent'] = (final_events1['observed'] / final_events1['at_risk'])*100
    final_events1['group'] = control_group_label
    print(final_events1)
    ################################

    # Next plot the treatment group
    f2 = KaplanMeierFitter()
    f2.fit(durations = df.loc[(df[treatment] == 1), time], 
    event_observed = df.loc[(df[treatment] == 1), outcome],
    label = treatment_group_label, 
    # weights = df.loc[(df[treatment] == 1), 'weight']
    )

    # Plot the cumulative density for 1 group
    f2.plot_cumulative_density(ax = ax, ci_alpha = 0.1, ci_show = True)

    ############### Get lifetable
    table2 = survival_table_from_events(death_times = df.loc[(df[treatment] == 1), time], 
                                    event_observed = df.loc[(df[treatment] == 1), outcome],
    #                                    intervals = 4
                                        )

    ### GET CUMULATIVE EVENTS AND PERCENT DIED
    final_events2 = pd.DataFrame(table2.agg({'at_risk': lambda x: x.iloc[1], 'observed':np.sum})).T
    final_events2['percent'] = (final_events2['observed'] / final_events2['at_risk'])*100
    final_events2['group'] = treatment_group_label
    print(final_events2)
    ################################

    from lifelines.statistics import logrank_test
    logrank1 = logrank_test(df.loc[(df[treatment] == 0), time], 
    df.loc[(df[treatment] == 1), time], 
    event_observed_A = df.loc[(df[treatment] == 0), outcome], 
    event_observed_B = df.loc[(df[treatment] == 1), outcome],
    # weights_A = df.loc[(df[treatment] == 0), 'weight'], weights_B = df.loc[(df[treatment] == 1), 'weight']
    )
    print(logrank1)

    # Set the limits of the X-axis
    ax.set_xlim(0, max_month)
    ax.set_xticks(np.linspace(0, max_month, max_month+1))
    ax.set_xlabel(x_axis_label)
    # ax.set_ylim(0, 0.17)

    # Save the X-axis labels
    # labels = ax.get_xticklabels()
    # labels = ax.get_xticks() # We use xticks since the Xaxis is numeric
    
    # # Add legend
    # ax.legend([control_group_label, treatment_group_label])

    
    
    ####### FINAL PLOT EDITS ###################
    ax.set_title(inset_title)
    plt.suptitle(title, fontsize = 12, fontweight = 'bold')
    ax.set_ylabel('Cumulative Incidence (%)')
    plt.show()

    return df
    

@transform_pandas(
    Output(rid="ri.vector.main.execute.c7910370-b8b3-45d8-abd6-b6f00fdb1d55"),
    paxlovid_long_covid_coxreg_prep=Input(rid="ri.foundry.main.dataset.c1562765-12f3-4715-baa4-c02852eabc73")
)
def Paxlovid_KM_Curve_LongCovid(paxlovid_long_covid_coxreg_prep):

    time = 'long_covid_time'
    event = 'outcome'
    title = 'Risk of Long COVID'
    inset_title = 'By 180 days'
    treatment_group_label = 'Paxlovid'
    control_group_label = 'Untreated'
    x_axis_label = 'Day'

    #################
    treatment = 'treatment'

    df = paxlovid_long_covid_coxreg_prep
    df = df.select('person_id',treatment,time,event).toPandas()
    
    

    #### STEP 1 - GET THE LIFETABLE - N AT RISK
    from lifelines.utils import survival_table_from_events
    from lifelines import NelsonAalenFitter, KaplanMeierFitter
    from matplotlib.gridspec import GridSpec
    import matplotlib.table as tbl
    from lifelines.plotting import add_at_risk_counts
    
    fig, ax = plt.subplots(1,1, figsize = (6,5))
    set_output_image_type('svg')

    ###### 1. PLOT FOR EARLY TREATMENT ("person_id","weight","treatment","time","outcome")
    # First plot no treatment
    f1 = KaplanMeierFitter()
    f1.fit(durations = df.loc[(df[treatment] == 0), time], 
    event_observed = df.loc[(df[treatment] == 0), event],
    label = control_group_label, 
    # weights = df.loc[(df[treatment] == 0), 'weight'], 
    )

    # Plot the cumulative density for 1 group
    f1.plot_cumulative_density(ax = ax, ci_show = True)

    

    ############### Get lifetable
    table1 = survival_table_from_events(death_times = df.loc[(df[treatment] == 0), time], 
                                    event_observed = df.loc[(df[treatment] == 0), event],
    #                                    intervals = 4
                                        )

    ### GET CUMULATIVE EVENTS AND PERCENT DIED
    final_events1 = pd.DataFrame(table1.agg({'at_risk': lambda x: x.iloc[1], 'observed':np.sum})).T
    final_events1['percent'] = (final_events1['observed'] / final_events1['at_risk'])*100
    final_events1['group'] = control_group_label
    print(final_events1)
    ################################

    # Next plot the treatment group
    f2 = KaplanMeierFitter()
    f2.fit(durations = df.loc[(df[treatment] == 1), time], 
    event_observed = df.loc[(df[treatment] == 1), event],
    label = treatment_group_label, 
    # weights = df.loc[(df[treatment] == 1), 'weight']
    )

    # Plot the cumulative density for 1 group
    f2.plot_cumulative_density(ax = ax, ci_alpha = 0.1, ci_show = True)

    ############### Get lifetable
    table2 = survival_table_from_events(death_times = df.loc[(df[treatment] == 1), time], 
                                    event_observed = df.loc[(df[treatment] == 1), event],
    #                                    intervals = 4
                                        )

    ### GET CUMULATIVE EVENTS AND PERCENT DIED
    final_events2 = pd.DataFrame(table2.agg({'at_risk': lambda x: x.iloc[1], 'observed':np.sum})).T
    final_events2['percent'] = (final_events2['observed'] / final_events2['at_risk'])*100
    final_events2['group'] = treatment_group_label
    print(final_events2)
    ################################

    from lifelines.statistics import logrank_test
    logrank1 = logrank_test(df.loc[(df[treatment] == 0), time], 
    df.loc[(df[treatment] == 1), time], 
    event_observed_A = df.loc[(df[treatment] == 0), event], 
    event_observed_B = df.loc[(df[treatment] == 1), event],
    # weights_A = df.loc[(df[treatment] == 0), 'weight'], weights_B = df.loc[(df[treatment] == 1), 'weight']
    )
    print(logrank1)

    # Set the limits of the X-axis
    ax.set_xlim(0, 180)
    ax.set_xticks(np.linspace(0, 180, 10))
    ax.set_xlabel(x_axis_label)
    # ax.set_ylim(0, 0.17)

    # Save the X-axis labels
    # labels = ax.get_xticklabels()
    # labels = ax.get_xticks() # We use xticks since the Xaxis is numeric
    
    # # Add legend
    # ax.legend([control_group_label, treatment_group_label])

    
    
    ####### FINAL PLOT EDITS ###################
    ax.set_title(inset_title)
    plt.suptitle(title, fontsize = 12, fontweight = 'bold')
    ax.set_ylabel('Cumulative Incidence (%)')
    plt.show()

    ####### FINAL PLOT EDITS ###################
    ax.set_title(inset_title)
    plt.suptitle(title, fontsize = 12, fontweight = 'bold')
    ax.set_ylabel('Cumulative Incidence (%)')
    plt.show()

    outcome = 'Long COVID'

    dffinal = pd.concat([
	final_events1, 
	final_events2, 
	# final_events3, 
	# final_events4, 
	# final_events5, 
	# final_events6
	])
    dffinal['drug'] = treatment_group_label
    dffinal['outcome'] = outcome
    # set_output_image_type('svg')
    # return dffinal
    
    return dffinal
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e9893d4a-c35a-4b2f-82b1-8a7101c41cd4"),
    testing_balance_copied_1=Input(rid="ri.foundry.main.dataset.3ce9c84e-3810-4a14-93c3-b73000249aa8")
)
def balance_plot_matching(testing_balance_copied_1):
    paxlovid_testing_balance = testing_balance_copied_1

    sb_df = paxlovid_testing_balance
    sb_df = sb_df.query('variable != "logit"') 
    sb_df = sb_df.query('variable != "distance"')

    # # Take absolute values of the columns
    # sb_df[['prematched_SB', 'matched_SB']] = sb_df[['prematched_SB', 'matched_SB']].apply(np.abs)

    # Replace the variable names
    variable_names = sb_df['variable'].unique()

#     # Set up the name replacement dictionary
#     colmap = {
#         'Age':'Age at COVID-19 diagnosis'
# }

#     # Replace the variable column
#     sb_df['variable'] = sb_df['variable'].replace(colmap)

    # Now we can produce the SMD plot - First- restructure the data frame to be stacked (all SMDs in one column, separate rows for pre and post matched SMD)
    smd = sb_df
    smd = smd.replace([np.inf, -np.inf], np.nan)
    smd = smd.sort_values(by = ["prematched_SB"], ascending = False).dropna()
    smd = smd.set_index('variable')
    smd = smd.stack()
    smd = pd.DataFrame(smd)
    smd = smd.reset_index()
    smd.columns = ['variable','sample','SMD']
    smd['sample'] = smd['sample'].replace({'prematched_SB':'Before Matching','matched_SB':'After Matching'})
    smd.head()

    # ## PLOT THE CHART
    # set_output_image_type('svg')

    ############ SINGLE PLOT
    ###### SET UP PLOT
    fig, ax = plt.subplots(1, figsize=(15, 30))

    ###### PLOT PRE AND POST
    sns.stripplot(x="SMD", y="variable", data=smd, ax=ax, hue = 'sample')
    ax.grid(axis='y')
    ax.axvline(x=0.1, linewidth=0.5, color='black', linestyle='--')
    # ax.axvline(x=-0.1, linewidth=0.5, color='black', linestyle='--')
    # plot the vertical lines for the MAXIMUM SMD
    ax.axvline(x=0.25, linewidth=0.5, color='red', linestyle='--')
    # ax.axvline(x=-0.25, linewidth=0.5, color='red', linestyle='--')

    ax.axvline(x=-0, linewidth=0.5, color='black', linestyle='-')

    # ax.annotate('', xy=(0, 1.01), xytext=(0.31, 1.01), xycoords='axes fraction', arrowprops=dict(arrowstyle="->", color='black'))
    # ax.annotate('Lower for Paxlovid users', xy=(0, 1.02), xytext=(0.05, 1.02), xycoords='axes fraction')

    # ax.annotate('', xy=(1.0, 1.01), xytext=(0.31, 1.01), xycoords='axes fraction', arrowprops=dict(arrowstyle="->", color='black'))
    # ax.annotate('Higher for Paxlovid users', xy=(1.0, 1.02), xytext=(0.31, 1.02), xycoords='axes fraction')

    #ax.text(0.1, len(df_smd), '0.1')
    # ax.set_yticklabels([col_map[x.get_text()] for x in ax.get_yticklabels()])
    ax.set_ylabel('')
    ax.set_xlabel('Absolute Standardized Mean Difference')
    # ax.legend(['Before Matching', 'After Matching'])
    
    # plt.suptitle('Combined Cohort', fontsize = 12, fontweight = 'bold')
    plt.tight_layout(rect=(0, 0, 1, 0.95))
    plt.show()

    return smd
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e910efc8-0261-40da-86f1-2f90e2d5e44b"),
    oxygen_date=Input(rid="ri.foundry.main.dataset.91a6e009-64ca-466c-b3ba-34b2a086b9f8"),
    visits_simpler=Input(rid="ri.foundry.main.dataset.e1a48189-26ee-4b58-9160-fa38aaafa5d3")
)
def baseline_variables_stacked(oxygen_date, visits_simpler):
    visits_simpler = visits_simpler

    # The purpose of this node is to aggregate covariates for each trial and stack these. Different covariates have different lookback periods, anchored to the trial day
    n_trials = 5
    final_data_frames = []
    
    # List of treatment exposures
    treatment_exposures = ["paxlovid_treatment"]
    treatment_exposures = [column for column in treatment_exposures if column in visits_simpler.columns]
    print('treatments', treatment_exposures)

    for trial_number in np.arange(0, n_trials + 1):
        print('trial_number:', trial_number)
        
        df = visits_simpler.withColumn('COVID_first_poslab_or_diagnosis_date', expr('DATE_ADD(COVID_first_poslab_or_diagnosis_date, {})'.format(trial_number)))
        df = df.where(expr('date <= COVID_first_poslab_or_diagnosis_date')).orderBy('person_id','date')
        dfever_exclude_day_of_covid = df.where(expr('date <= DATE_ADD(COVID_first_poslab_or_diagnosis_date, -1)')) # For Paxlovid - we don't want anyone taking the drug EVER before
        df28 = df.where(expr('date BETWEEN DATE_ADD(COVID_first_poslab_or_diagnosis_date, -28) AND COVID_first_poslab_or_diagnosis_date'))
        df30 = df.where(expr('date BETWEEN DATE_ADD(COVID_first_poslab_or_diagnosis_date, -30) AND COVID_first_poslab_or_diagnosis_date'))
        df45 = df.where(expr('date BETWEEN DATE_ADD(COVID_first_poslab_or_diagnosis_date, -45) AND COVID_first_poslab_or_diagnosis_date'))
        df90 = df.where(expr('date BETWEEN DATE_ADD(COVID_first_poslab_or_diagnosis_date, -90) AND COVID_first_poslab_or_diagnosis_date'))
        df270 = df.where(expr('date BETWEEN DATE_ADD(COVID_first_poslab_or_diagnosis_date, -270) AND COVID_first_poslab_or_diagnosis_date'))
        df365 = df.where(expr('date BETWEEN DATE_ADD(COVID_first_poslab_or_diagnosis_date, -365) AND COVID_first_poslab_or_diagnosis_date'))
        # df365_treatments = df.where(expr('date BETWEEN DATE_ADD(COVID_first_poslab_or_diagnosis_date, -366) AND DATE_ADD(COVID_first_poslab_or_diagnosis_date, -1)'))
        

        # Below, create a data frame that will lookback 6 months and look forward 3 months to extract A1c data for comparison
        df180 = df.where(expr('date BETWEEN DATE_ADD(COVID_first_poslab_or_diagnosis_date, -180) AND DATE_ADD(COVID_first_poslab_or_diagnosis_date, 90)'))

        # For covariates, take the max value (conditions and medications, and drug and condition interactions)
        # For BMI take most recent value. 
        # For vaccination - take the SUM of vaccinated. THEN rename to the vaccinated_dose_count
        # For lab values and BMI take the most recent
        # For interactions - diseases OR medications - take the max value

        # For indicated conditions take the max value (diabetes only)
        # For indicated drugs (for diabetes only) - take the max value in the last 12 months
        
        # For contraindicated conditions take the max value, except for current conditions like pregnancy (take 9 months) and loaloa (28 days) and onchocerciasis infection
        # For contraindicated medications take the max value in the last 45 days

        # Other COVID tx - take the max value
        # For the TREATMENTS - last 12 months
        # We will worry about hospitalizations, ED visits, and deaths later

        # Set up variable lists
        # For covariates, medications, interactions, and exclusion indications/contraindicated diseases - ever (max)
        covariates = [column for column in df.columns if ("risk_factor" in column) | ("procedure" in column) | ("sdoh2" in column) | ("cci_index_covariate" in column) | ("_condition" in column) & ('exclusion' not in column)]
        medications_covariates = [column for column in df.columns if 'covariate_drug_exposure' in column]
        
        # BMI take most recent value
        bmi_columns = ['BMI_rounded']

        # For vaccination columns, take the SUM
        vaccination_columns = [column for column in df.columns if (column == "vaccinated")]

        # Lab values - take most recent value
        lab_values = ["CRP","WBC","LYMPH","ALBUMIN","ALT","EGFR","DIABETES_A1C","HEMOGLOBIN","CREATININE","PLT"]

        # Interactions - ever (max)
        interactions_covariates = [column for column in df.columns if ('covariate_drug_interaction' in column) | ('covariate_disease_interaction' in column) | ('relatively_contraindicated' in column)]

        # Contraindicated conditions - ever
        exclusion_contraindicated_diseases = [column for column in df.columns if ('exclusion_contraindication_condition' in column)]
        
        # Contraindicated medications - all 45 days
        exclusion_contraindicated_medications = [column for column in df.columns if ('exclusion_contraindication_drug' in column)]
        
        # For other covid19 treatments - 28 days
        other_covid19_treatments = [column for column in df.columns if "covid19_treatment" in column]

        ####### PERFORM AGGREGATIONS ##################
        # # Perform aggregations [these all apply to EVER]
        # columns_to_max = covariates + medications_covariates + interactions_covariates + exclusion_indications_conditions + exclusion_contraindicated_diseases
        columns_to_max = covariates + medications_covariates + interactions_covariates + exclusion_contraindicated_diseases
        max_aggregations = [max(col(column)).alias(column) for column in columns_to_max]
        sum_aggregations = [sum(col(column)).alias(column) for column in vaccination_columns] 
        last_aggregations = [last(col(column), ignorenulls = True).alias(column) for column in lab_values + bmi_columns]
        df_agg = df.groupBy('person_id','COVID_first_poslab_or_diagnosis_date').agg(*max_aggregations, *sum_aggregations, *last_aggregations)

        # For EGFR (lab value) - we will look back 365 days - the suffix "current" will be used
        columns_to_max = ['EGFR']
        max_aggregations = [max(col(column)).alias(column + '_current') for column in columns_to_max]
        df_egfr = df365.groupBy('person_id','COVID_first_poslab_or_diagnosis_date').agg(*max_aggregations)

        # # For A1c - we will look back 12 months THROUGH day of trial
        # columns_to_max = ['DIABETES_A1C']
        # max_aggregations = [max(col(column)).alias(column + '_current') for column in columns_to_max]
        # df_A1c = df180.groupBy('person_id','COVID_first_poslab_or_diagnosis_date').agg(*max_aggregations)
        # last_aggregations = [last(col(column), ignorenulls = True).alias(column + '_current') for column in columns_to_max]
        # df_A1c = df365.groupBy('person_id','COVID_first_poslab_or_diagnosis_date').agg(*last_aggregations)

        # # Repeat for the prior drug indications - 365 days (diabetes drugs)
        # columns_to_max = exclusion_indications_drugs
        # max_aggregations = [max(col(column)).alias(column) for column in columns_to_max]
        # df_agg365 = df365.groupBy('person_id','COVID_first_poslab_or_diagnosis_date').agg(*max_aggregations)

        # Repeat for drug contraindications - 90 days
        columns_to_max = exclusion_contraindicated_medications
        max_aggregations = [max(col(column)).alias(column) for column in columns_to_max]
        df_contraindicated_treatment = df90.groupBy('person_id','COVID_first_poslab_or_diagnosis_date').agg(*max_aggregations)

        # # Repeat for current condition contraindications AND current condition indications - we will include COVID treatments here - 28 days
        # columns_to_max = exclusion_contraindicated_diseases_current + other_covid19_treatments + exclusion_indications_conditions_current
        columns_to_max = other_covid19_treatments
        max_aggregations = [max(col(column)).alias(column) for column in columns_to_max]
        df_other_covid_tx = df30.groupBy('person_id','COVID_first_poslab_or_diagnosis_date').agg(*max_aggregations)

        # # Repeat for the treatment drugs - lookback 365 days. This should exclude the day of the trial. 
        # columns_to_max = treatment_exposures
        # max_aggregations = [max(col(column)).alias(column) for column in columns_to_max]
        # df_prior_treatment = df365_treatments.groupBy('person_id','COVID_first_poslab_or_diagnosis_date').agg(*max_aggregations)

        # Repeat for the treatment drugs - lookback 365 days. This should exclude the day of the trial. 
        columns_to_max = treatment_exposures
        max_aggregations = [max(col(column)).alias(column) for column in columns_to_max]
        df_prior_treatment = dfever_exclude_day_of_covid.groupBy('person_id','COVID_first_poslab_or_diagnosis_date').agg(*max_aggregations)

        # # Repeat for pregnancy - 270 days
        # columns_to_max = exclusion_contraindicated_diseases_pregnancy
        # max_aggregations = [max(col(column)).alias(column) for column in columns_to_max]
        # df_agg270 = df270.groupBy('person_id','COVID_first_poslab_or_diagnosis_date').agg(*max_aggregations)

        # # We now have all the variables we need; join the tables together
        # final = df_prior_treatment.join(df_agg, on = ['person_id','COVID_first_poslab_or_diagnosis_date'], how = 'inner').join(df_egfr, on = ['person_id','COVID_first_poslab_or_diagnosis_date'], how = 'inner').join(df_A1c, on = ['person_id','COVID_first_poslab_or_diagnosis_date'], how = 'inner').join(df_agg365, on = ['person_id','COVID_first_poslab_or_diagnosis_date'], how = 'inner').join(df_contraindicated_treatment, on = ['person_id','COVID_first_poslab_or_diagnosis_date'], how = 'inner').join(df_other_covid_tx, on = ['person_id','COVID_first_poslab_or_diagnosis_date'], how = 'inner').withColumnRenamed('COVID_first_poslab_or_diagnosis_date','date')

        # We now have all the variables we need; join the tables together
        final = df_prior_treatment.join(df_agg, on = ['person_id','COVID_first_poslab_or_diagnosis_date'], how = 'inner').join(df_egfr, on = ['person_id','COVID_first_poslab_or_diagnosis_date'], how = 'inner').join(df_contraindicated_treatment, on = ['person_id','COVID_first_poslab_or_diagnosis_date'], how = 'inner').join(df_other_covid_tx, on = ['person_id','COVID_first_poslab_or_diagnosis_date'], how = 'inner').withColumnRenamed('COVID_first_poslab_or_diagnosis_date','date')

        final_data_frames.append(final)

    # Join the data frames
    from functools import reduce
    from pyspark.sql import DataFrame

    df_complete = reduce(DataFrame.unionAll, final_data_frames)

    return df_complete
    

    

    

    
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b6450c6b-55d3-41e3-8208-14fa0780cd41"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.900fa2ad-87ea-4285-be30-c6b5bab60e86"),
    location=Input(rid="ri.foundry.main.dataset.efac41e8-cc64-49bf-9007-d7e22a088318"),
    manifest_safe_harbor=Input(rid="ri.foundry.main.dataset.b1e99f7f-5dcd-4503-985a-bbb28edc8f6f"),
    measurement=Input(rid="ri.foundry.main.dataset.d6054221-ee0c-4858-97de-22292458fa19"),
    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.5af2c604-51e0-4afa-b1ae-1e5fa2f4b905"),
    person=Input(rid="ri.foundry.main.dataset.50cae11a-4afb-457d-99d4-55b4bc2cbe66")
)
from pyspark.sql.types import StructType, DateType, StructField, StringType, IntegerType, FloatType
from pyspark.sql import Row

# Import functions and tools to make functions, and interact with columns with dot functions or SQL functions
from pyspark.sql.functions import lower, upper, col, udf, monotonically_increasing_id, to_date, trim, ltrim, rtrim, avg
from pyspark.sql.functions import length, size, unix_timestamp, from_unixtime, broadcast, to_timestamp, split, when, rand, count
from pyspark.sql.functions import min, max, col, mean, lit, when, regexp_replace, lower, upper, concat_ws, to_date, floor, months_between, datediff, date_add, current_date, least

def cohort_code(measurement, concept_set_members, person, location, manifest_safe_harbor, condition_occurrence, microvisits_to_macrovisits):

    # Section 1 - Make some modifications to the input tables and rename them
    # select proportion of enclave patients to use
    proportion_of_patients_to_use = 1.0

    # Create copies of some of the above tables we are using to work with them in this function
    concepts_df = concept_set_members

    # PERSON TABLE [IMPORTANT] - ******WE WILL WANT TO LIMIT THIS TABLE TO A PRE-PERSON TABLE BASED ON OUR SELECTION CRITERIA***** 
    person_sample = person.select('person_id','year_of_birth','month_of_birth','day_of_birth','ethnicity_concept_name','race_concept_name','gender_concept_name','location_id','data_partner_id').withColumnRenamed('gender_concept_name', 'sex').distinct().sample(withReplacement = False, fraction = proportion_of_patients_to_use, seed = 111)

    ######### NEW - FILTER TO VACCINE VALIDATED SITES #####################
    # good_data_partners = [770, 439, 198, 793, 124, 726, 507, 183] # Vaccine-validated sites
    bad_data_partners = [23,41,170,605,888,181,966,565,1015,578,224,901,38]
    # person_sample = person_sample.where(col('data_partner_id').isin(good_data_partners))
    person_sample = person_sample.where(~col('data_partner_id').isin(bad_data_partners))

    # This table is exclusive to the COVID+ve cohort; JOIN TO PERSON TABLE
    measurement_df = measurement.select('person_id', 'measurement_date', 'measurement_concept_id', 'value_as_concept_id').where(col('measurement_date').isNotNull()).join(person_sample, on = 'person_id', how = 'inner')

    # This table is exclusive to COVID+ve; JOIN TO PERSON TABLE (COMPLETE)
    conditions_df = (condition_occurrence.select('person_id', 'condition_start_date', 'condition_concept_id').where(col('condition_start_date').isNotNull()).join(person_sample, 'person_id','inner'))

    # TABLE OF VISITS - RELEVANT TO EVERYONE
    visits_df = microvisits_to_macrovisits.select('person_id', 'macrovisit_start_date','visit_start_date','likely_hospitalization','visit_concept_name')
    
    manifest_df = manifest_safe_harbor.select('data_partner_id','run_date','cdm_name','cdm_version','shift_date_yn','max_num_shift_days').withColumnRenamed('run_date', 'data_extraction_date')
    
    location_df = location.dropDuplicates(subset=['location_id']).select('location_id','city','state','zip','county').withColumnRenamed('zip','postal_code')

    # Section 2: Identify who has a COVID+ test
    ##### THIS IS EXLUSIVE TO COV+ PATIENTS - CREATE A TEMP TABLE TO IDENTIFY WHO HAS A COV+ TEST
    # 1. Make a list of concept sets for COVID TESTS. Convert to a pandas list
    covid_measurement_test_ids = list(concepts_df.where((col('concept_set_name')=='ATLAS SARS-CoV-2 rt-PCR and AG') & (col('is_most_recent_version')=='true')).select('concept_id').toPandas()['concept_id'])
    
    # 2. Make a list of the concept set names for POSITIVE COVID test results. Convert this to a PANDAS list
    covid_positive_measurement_ids = list(concepts_df.where((col('concept_set_name')=='ResultPos') & (col('is_most_recent_version')=='true')).select('concept_id').toPandas()['concept_id'])
    
    # 3. Now search the measurement table for the ABOVE COVID tests in the list
    measurements_of_interest = measurement_df.where(col('measurement_concept_id').isin(covid_measurement_test_ids))
    
    # 4. Filter the ABOVE table with COVID tests for where the RESULT is +ve
    measurements_of_interest = measurements_of_interest.where(col('value_as_concept_id').isin(covid_positive_measurement_ids)).withColumnRenamed('measurement_date','covid_measurement_date').dropDuplicates(subset=['person_id','covid_measurement_date']).select(col('person_id'), col('covid_measurement_date'))
    
    # 5. Now we take 1 row per person and select the minimum (earliest date that had a COVID positive test)
    first_covid_pos_lab = measurements_of_interest.groupBy('person_id').agg(min(col('covid_measurement_date')).alias('COVID_first_PCR_or_AG_lab_positive'))

    # Section 3: Identify who has been diagnosed with COVID
    ###### COVID DIAGNOSIS: SECTION TO ADD A FLAG FOR THE FIRST COVID DIAGNOSIS (COVID DIAGNOSIS)
    # 1. Create a list of concept sets referring to COVID diagnosis
    COVID_concept_ids = list(concepts_df.where((col('concept_set_name')=='N3C Covid Diagnosis') & (col('is_most_recent_version')=='true')).select('concept_id').toPandas()['concept_id'])
    
    # 2. Filter Conditions Occurrence table to the COVID diagnosis concept set list above in 1.
    conditions_of_interest = conditions_df.where(col('condition_concept_id').isin(COVID_concept_ids)).withColumnRenamed('condition_start_date','covid_DIAGNOSIS_date').dropDuplicates(subset=['person_id','covid_DIAGNOSIS_date']).select(col('person_id'),col('covid_DIAGNOSIS_date'))
    
    # 3. Now we take 1 row per person and select the minimum (earliest date for a COVID diagnosis)
    first_covid_DIAGNOSIS = conditions_of_interest.groupBy('person_id').agg(min(col('covid_DIAGNOSIS_date')).alias('COVID_first_diagnosis_date'))

    # Section 4. Determine who is COVID+ (either a positive test or a diagnoses) and determine the date - the is the INDEX DATE
    # Outer join will create 1 row per patient_id with two columns (COVID_first_diagnosis_date, COVID_first_PCR_or_AG_lab_positive). Not all patients will have values in each column 
    df = first_covid_pos_lab.join(first_covid_DIAGNOSIS, on = 'person_id', how = 'outer')
    
    #### Choose the earlier date for either (COVID_first_diagnosis_date, COVID_first_PCR_or_AG_lab_positive) for the index date
    df = df.withColumn('COVID_first_poslab_or_diagnosis_date', least(col('COVID_first_diagnosis_date'), col('COVID_first_PCR_or_AG_lab_positive')))
    
    # Section 5. Join COVID+ and ALL patients
    # 1. Join the COVID positive patients to our person_sample table. If want to limit to COV+ patients make this join INNER
    use_only_covid_positive = True
    if use_only_covid_positive == True:
        df = person_sample.join(df, on = 'person_id', how = 'inner')
    else:
        df = person_sample.join(df, on = 'person_id', how = 'left')

    # Join to the location dataset
    df = df.join(location_df, on = 'location_id', how = 'left')

    #### Add indicator for a person who has been diagnosed with COVID
    df = df.withColumn('COVID_EVER', when(col('COVID_first_poslab_or_diagnosis_date').isNotNull(), 1).otherwise(0))
    
    # Section 6. Join df above to our manifest table (which is a table about data partners). Includes details about how much date shifting occurred. 
    # 1. Calculate each patient's maximum shift. The query below: when max_num_shift_days is blank, then add a text string '0' - otherwise use the existing value of the max_num_shift_days column (after first replacing 'na' with 0)
    df = df.join(manifest_df, on = 'data_partner_id', how = 'inner')
    df = df.withColumn('max_num_shift_days', when(col('max_num_shift_days') == '', lit('0')).otherwise(regexp_replace(lower(col('max_num_shift_days')), 'na', '0')))
    

    # Section 7. Calculate DOB Correctly - multiple conditionals are incorporated depending on whether or not the year, month, and day is absent in their respective columns
    # 1. First replace missing values with 1, 7, and 7 for the year, month, and day respectively 
    df = df.withColumn('new_year_of_birth', when(col('year_of_birth').isNull(), 1).otherwise(col('year_of_birth')))
    df = df.withColumn('new_month_of_birth', when(col('month_of_birth').isNull(), 7).otherwise(when(col('month_of_birth') == 0, 7).otherwise(col('month_of_birth'))))
    df = df.withColumn('new_day_of_birth', when(col('day_of_birth').isNull(), 1).otherwise(when(col('day_of_birth') == 0, 7).otherwise(col('day_of_birth'))))
    
    # Now concatenate the above columns into a date of birth column (with dash in between), then convert to date
    df = df.withColumn('date_of_birth', concat_ws('-', col('new_year_of_birth'), col('new_month_of_birth'), col('new_day_of_birth')))
    df = df.withColumn('date_of_birth', to_date(col('date_of_birth'), format = None))
    
    # Section 8. Modify the date of birth column and use it to calculate age and age_at_covid
    # 1. First, create an object that is the maximum number days a patient may have had data shifted. Add this to the current date and this will form the maximum reasonable date of birth 
    max_shift_as_int = df.select(max(col('max_num_shift_days').cast(IntegerType()))).head()[0]
    max_reasonable_dob = date_add(current_date(), max_shift_as_int)
    
    # 2. Create an object for the minimum reasonable date of birth
    min_reasonable_dob = '1902-01-01'
    
    # 3. Then finally - if the current date_of_birth falls in between these values, keep it, otherwise make it null
    df = df.withColumn('date_of_birth', when(col('date_of_birth').between(min_reasonable_dob, max_reasonable_dob), col('date_of_birth')).otherwise(None))
    
    # 4. Next calculate the age at COVID (in years). First calculate the number of months between COVID_first_poslab_or_diagnosis_date and date of birth. We don't round the value to 8 digits (the default), so roundoff = False. We divide that number by 12 to get the age in years at COVID [index date]
    df = df.withColumn('age_at_covid', floor(months_between(col('COVID_first_poslab_or_diagnosis_date'), col('date_of_birth'), roundOff=False)/12))
    
    # 5. New column to calculate patient age. Have added this in: create a column for current age
    df = df.withColumn('age', floor(months_between(max_reasonable_dob, 'date_of_birth', roundOff=False)/12))
    
    # # 6. Add in a new section to calculate age_at_baseline
    # max_dob = date_add(to_date('2021-08-01'), max_shift_as_int)
    # df = df.withColumn('age_at_baseline', floor(months_between(max_dob, col('date_of_birth'), roundOff = False)/12))

    # Section 9. Ethnicity and Race
    H = ['Hispanic']
    A = ['Asian', 'Asian Indian', 'Bangladeshi', 'Bhutanese', 'Burmese', 'Cambodian', 'Chinese', 'Filipino', 'Hmong', 'Indonesian', 'Japanese', 'Korean', 'Laotian', 'Malaysian', 'Maldivian', 'Nepalese', 'Okinawan', 'Pakistani', 'Singaporean', 'Sri Lankan', 'Taiwanese', 'Thai', 'Vietnamese']
    B_AA = ['African', 'African American', 'Barbadian', 'Black', 'Black or African American', 'Dominica Islander', 'Haitian', 'Jamaican', 'Madagascar', 'Trinidadian', 'West Indian']
    W = ['White']
    NH_PI = ['Melanesian', 'Micronesian', 'Native Hawaiian or Other Pacific Islander', 'Other Pacific Islander', 'Polynesian']
    AI_AN = ['American Indian or Alaska Native']
    O = ['More than one race', 'Multiple race', 'Multiple races', 'Other', 'Other Race']
    U = ['Asian or Pacific Islander', 'No Information', 'No matching concept', 'Refuse to Answer', 'Unknown', 'Unknown racial group']
    
    # 1. Below we create new column 'race' and 'race_ethnicity' using the 'race_concept_name' column. Converting to the value specified in the case statements provided
    df = df.withColumn('race', when(col('race_concept_name').isin(H), 'Hispanic or Latino').when(col('race_concept_name').isin(A), 'Asian').when(col('race_concept_name').isin(B_AA), 'Black or African American').when(col('race_concept_name').isin(W), 'White').when(col('race_concept_name').isin(NH_PI), 'Native Hawaiian or Other Pacific Islander').when(col('race_concept_name').isin(AI_AN), 'American Indian or Alaska Native').when(col('race_concept_name').isin(O), 'Other').when(col('race_concept_name').isin(U), 'Unknown').otherwise('Unknown'))

    df = df.withColumn('race_ethnicity', when(col('ethnicity_concept_name') == 'Hispanic or Latino', 'Hispanic or Latino Any Race').when(col('race_concept_name').isin(H), 'Hispanic or Latino Any Race').when(col('race_concept_name').isin(A), 'Asian Non-Hispanic').when(col('race_concept_name').isin(B_AA), 'Black or African American Non-Hispanic').when(col('race_concept_name').isin(W), 'White Non-Hispanic').when(col('race_concept_name').isin(NH_PI), 'Native Hawaiian or Other Pacific Islander Non-Hispanic').when(col('race_concept_name').isin(AI_AN), 'American Indian or Alaska Native Non-Hispanic').when(col('race_concept_name').isin(O), 'Other Non-Hispanic').when(col('race_concept_name').isin(U), 'Unknown').otherwise('Unknown'))

    # Section 10. Count Hospital macrovisits and observation periods (in days) before and after COVID
    # 1. Identify Hospital Macro-Visits (i.e., where macrovisit_start_date is not null - WE ARE ONLY INCLUDING MACROVISITS). Order rows by visit_start_date
    hosp_visits = visits_df.where(expr('(macrovisit_start_date IS NOT NULL) AND (likely_hospitalization = 1)')).orderBy('visit_start_date').coalesce(1).dropDuplicates(['person_id', 'macrovisit_start_date'])
    hosp_visits = hosp_visits.withColumn('hosp_visit', lit(1)).withColumn('ED_visit', lit(0)).select('person_id','visit_start_date','hosp_visit','ED_visit')
    
    # 2. Identify non hospital visits - (i.e., where macrovisit_start_date is null). Add a column to flag the visit is NOT a hospital visit
    non_hosp_visits = visits_df.where(expr('macrovisit_start_date IS NULL')).dropDuplicates(['person_id', 'visit_start_date'])
    non_hosp_visits = non_hosp_visits.withColumn('hosp_visit', lit(0)).withColumn('ED_visit', lit(0)).select('person_id','visit_start_date','hosp_visit','ED_visit')

    # 3. New section - aims to identify past ED visits. Add a column to flag declaring the visit is an ED visit; 
    ED_visits = visits_df.where(expr("visit_concept_name IN ('Emergency Room and Inpatient Visit', 'Emergency Room - Hospital', 'Emergency Room Visit', 'Emergency Room Critical Care Facility')")).dropDuplicates(['person_id', 'visit_start_date'])
    ED_visits = ED_visits.withColumn('hosp_visit', lit(0)).withColumn('ED_visit', lit(1)).select('person_id','visit_start_date','hosp_visit','ED_visit')

    # Append the two tables together
    visits_df = hosp_visits.union(non_hosp_visits).union(ED_visits)

    # 4. Merge the visits table above to df [many to 1], to get the COVID diagnosis/test date. Calculate the difference in days between each visit and when they got COVID
    # Datediff is (LATER (COVID date) - EARLY (visit date)). Thus visits occurring AFTER COVID diagnosis will have negative days; and visits occurring before COVID diagnosis will have positive days. 
    visits_df = visits_df.join(df.select('person_id','COVID_first_poslab_or_diagnosis_date','shift_date_yn','max_num_shift_days'), on = 'person_id', how = 'inner').withColumn('earliest_index_minus_visit_start_date', datediff('COVID_first_poslab_or_diagnosis_date','visit_start_date'))
    
    # 5. Count the number of visits before COVID. Filter to where the difference above is above 0; then count number of visits (for each patient)
    visits_before = visits_df.where(col('earliest_index_minus_visit_start_date') > 0).groupBy('person_id').count().select('person_id', col('count').alias('number_of_visits_before_covid')) 
    
    # 6. Calculate patients' total observation period (i.e., number of days in between first and last visit before/up to when they got COVID)
    # First, filter to visits occurring before or on diagnosis. Grouping by patients, subtract their first visit date from their last visit date before COVID
    
    observation_before = visits_df.where(col('earliest_index_minus_visit_start_date') >= 0).groupby('person_id').agg(max('visit_start_date').alias('pt_max_visit_date'), min('visit_start_date').alias('pt_min_visit_date')).withColumn('observation_period_before_covid', datediff('pt_max_visit_date', 'pt_min_visit_date')).select('person_id', 'observation_period_before_covid')
    
    # 7. Repeat Steps 5 and 6 for the visits POST COVID 
    visits_post = visits_df.where(col('earliest_index_minus_visit_start_date') < 0).groupBy('person_id').count().select('person_id', col('count').alias('number_of_visits_post_covid'))
    
    observation_post = visits_df.where(col('earliest_index_minus_visit_start_date') <= 0).groupby('person_id').agg(max('visit_start_date').alias('pt_max_visit_date'), min('visit_start_date').alias('pt_min_visit_date')).withColumn('observation_period_post_covid', datediff('pt_max_visit_date', 'pt_min_visit_date')).select('person_id', 'observation_period_post_covid')
    
    #### ********** NEW SECTION ************* ############
    # 1. We want to calculate the overall number of visits and the entire observation period
    visit_count = visits_df.groupBy('person_id').agg(count(col('person_id')).alias('total_visits'))
    
    # 2. Calculate the entire observation period of the patient from start to finish
    observation_period = visits_df.groupBy('person_id').agg(min(col('visit_start_date')).alias('pt_min_visit_date'), max(col('visit_start_date')).alias('pt_max_visit_date')).withColumn('observation_period', datediff('pt_max_visit_date', 'pt_min_visit_date')).select('person_id', 'observation_period')

    # 3. Count the number of visits (not just macrovisits) pre, during, and post index date
    hosp_visits_before = visits_df.where((col('earliest_index_minus_visit_start_date') > 0) & (col('hosp_visit') == 1)).groupBy('person_id').count().select('person_id', col('count').alias('total_hosp_visits_before_covid')) 
    hosp_visits_post = visits_df.where((col('earliest_index_minus_visit_start_date') < 0) & (col('hosp_visit') == 1)).groupBy('person_id').count().select('person_id', col('count').alias('total_hosp_visits_post_covid'))
    hosp_visit_count = visits_df.where(col('hosp_visit') == 1).groupBy('person_id').agg(count(col('person_id')).alias('total_hosp_visits'))

    # 4. Count the number of ED visits pre, during, and post index date (NEW SECTION)
    ED_visits_before = visits_df.where((col('earliest_index_minus_visit_start_date') > 0) & (col('ED_visit') == 1)).groupBy('person_id').count().select('person_id', col('count').alias('total_ED_visits_before_covid')) 
    ED_visits_post = visits_df.where((col('earliest_index_minus_visit_start_date') < 0) & (col('ED_visit') == 1)).groupBy('person_id').count().select('person_id', col('count').alias('total_ED_visits_post_covid'))
    ED_visit_count = visits_df.where(col('ED_visit') == 1).groupBy('person_id').agg(count(col('person_id')).alias('total_ED_visits'))
    #### ************************************ ############

    

    # Section 11: Join each of the visits tables above to our main df
    df = df.join(visits_before, 'person_id', 'left')
    df = df.join(observation_before, 'person_id', 'left')
    df = df.join(visits_post, 'person_id', 'left')
    df = df.join(observation_post, 'person_id', 'left')
    
    # Section 12: Join Section for overall visits count
    df = df.join(visit_count, on = 'person_id', how = 'left')
    df = df.join(observation_period, on = 'person_id', how = 'left')
    
    
    #### ********** NEW SECTION ************* ############
    df = df.join(hosp_visits_before, on = 'person_id', how = 'left')
    df = df.join(hosp_visits_post, on = 'person_id', how = 'left')
    df = df.join(hosp_visit_count, on = 'person_id', how = 'left')

    df = df.join(ED_visits_before, on = 'person_id', how = 'left')
    df = df.join(ED_visits_post, on = 'person_id', how = 'left')
    df = df.join(ED_visit_count, on = 'person_id', how = 'left')
    #### ************************************ ############

    
    # Section 13: Final Select Statement
    df = df.select(
        'person_id',
        'COVID_first_PCR_or_AG_lab_positive',
        'COVID_first_diagnosis_date',
        'COVID_first_poslab_or_diagnosis_date',
        'number_of_visits_before_covid',
        'observation_period_before_covid',
        'number_of_visits_post_covid',
        'observation_period_post_covid',
        'sex',
        'city',
        'state',
        'postal_code',
        'county',
        'age_at_covid',
        'race',
        'race_ethnicity',
        'data_partner_id',
        'data_extraction_date',
        'cdm_name',
        'cdm_version',
        'shift_date_yn',
        'max_num_shift_days',
        
        # New columns
        'age',
        'date_of_birth',
        'total_visits',
        'observation_period',

        # New columns for just hospital macrovisits (excluding normal visits)
        'total_hosp_visits_before_covid','total_hosp_visits_post_covid','total_hosp_visits',
        'total_ED_visits_before_covid','total_ED_visits_post_covid','total_ED_visits',
        
        )

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.63c6f325-61d9-4e4f-acc2-2e9e6c301adc"),
    Enriched_Death_Table_Final=Input(rid="ri.foundry.main.dataset.67ebd4b5-1028-4053-8924-06ce93a15509"),
    cohort_code=Input(rid="ri.foundry.main.dataset.b6450c6b-55d3-41e3-8208-14fa0780cd41"),
    conditions_filtered=Input(rid="ri.foundry.main.dataset.3af1cb46-7f36-4210-8519-70b0a8d54302"),
    devices_filtered=Input(rid="ri.foundry.main.dataset.a5c60b39-2f97-45d5-a01d-8bf4be84e48a"),
    drugs_filtered=Input(rid="ri.foundry.main.dataset.279e6680-9619-4a95-924d-90510ef7a9ab"),
    filtered_lab_measures_harmonized_cleaned_final=Input(rid="ri.foundry.main.dataset.90b57f36-aaeb-4f6d-8831-6141389595e0"),
    measurements_filtered=Input(rid="ri.foundry.main.dataset.2829da0b-d0cc-4a0e-8ecb-d8118849f408"),
    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.5af2c604-51e0-4afa-b1ae-1e5fa2f4b905"),
    minimal_patient_cohort=Input(rid="ri.foundry.main.dataset.9b0b693d-1e29-4eeb-aa02-a86f1c7c7fb6"),
    observations_filtered=Input(rid="ri.foundry.main.dataset.6151c319-494e-4b79-b05e-9533024a6174"),
    person_date_cross_join=Input(rid="ri.foundry.main.dataset.20a8907e-c506-4116-a04c-ad96436639f8"),
    procedures_filtered=Input(rid="ri.foundry.main.dataset.1537bb59-51b7-4765-a5b9-e78e6514d8f5"),
    vaccines_filtered=Input(rid="ri.foundry.main.dataset.69027164-230c-4bfa-b385-accf3b2b0482"),
    visits_filtered=Input(rid="ri.foundry.main.dataset.09c6fdb9-33f0-4d00-b71d-433cdd927eac")
)
def cohort_facts_visit_level_paxlovid(conditions_filtered, measurements_filtered, visits_filtered, procedures_filtered, observations_filtered, drugs_filtered, cohort_code, devices_filtered, microvisits_to_macrovisits, vaccines_filtered, filtered_lab_measures_harmonized_cleaned_final, Enriched_Death_Table_Final, minimal_patient_cohort, person_date_cross_join):
    person_date_cross_join = person_date_cross_join
    minimal_patient_cohort = minimal_patient_cohort

    # Set up all the person (cohort_code) tables
    # NOTE - we are also joining to MINIMAL PATIENT COHORT, which will help to reduce the number of patients in this dataset
    persons_df = cohort_code.select('person_id', 'COVID_first_PCR_or_AG_lab_positive', 'COVID_first_diagnosis_date', 'COVID_first_poslab_or_diagnosis_date').dropDuplicates()
    persons_df = persons_df.join(minimal_patient_cohort.select('person_id').distinct(), on = 'person_id', how = 'inner')
    
    INCLUDE_LAB = True
    lab_columns = ["BMI_rounded","CRP","WBC","LYMPH","ALBUMIN","ALT","EGFR","CREATININE","PLT","HEMOGLOBIN","DIABETES_A1C"]

    # Set up all the covariate tables
    macrovisits_df = microvisits_to_macrovisits # Multiple rows per patient
    vaccines_df = vaccines_filtered # Multiple rows per patient
    procedures_df = procedures_filtered # Multiple rows per patient
    devices_df = devices_filtered # Multiple rows per patient
    observations_df = observations_filtered # Multiple rows per patient
    conditions_df = conditions_filtered # Multiple rows per patient
    drugs_df = drugs_filtered # Multiple rows per patient
    measurements_df = measurements_filtered # Multiple rows per patient
    visits_df = visits_filtered # 1 row per patient [ED-related COVID visits]
    death_input = Enriched_Death_Table_Final

    # Extra - lab measurements
    # lab_df = filtered_lab_measures_harmonized
    lab_df = filtered_lab_measures_harmonized_cleaned_final

    
    # 1 row per patient. Filter to rows where patient died (date column not null), death date has to occur after Jan 2018 AND should occur before [extraction date + 2 years]
    deaths_df = death_input.where(
        (death_input.date.isNotNull()) 
        & (death_input.date >= "2018-01-01") 
        & (death_input.date < (col('data_extraction_date')+(365*2)))).drop('data_extraction_date')
        # & (death_input.date < (col('data_extraction_date')))).drop('data_extraction_date') # STEVE: Modified this condition, where the death date must occur before the extraction date; 
        
    # Each join is OUTER. Note that this table is the original macrovisit_to_microvisits table meaning it has ALL visits. Each pt has all their visits recorded [visits are nested in macrovisits]
    df = macrovisits_df.select('person_id','visit_start_date').withColumnRenamed('visit_start_date','date')
    
    # PERFORM OUTER JOINS BETWEEN TABLES: First, join macrovisit to vaccines_df. Both tables have multiple rows per patient (per visit)
    # For each merge - the merge is happening on the overlapping fields (due to the list and set syntax. This will be person_id and date
    # If the dates overlap between the two tables, they will join. If the tables don't overlap, the patient will have separate rows for each unique date in each joined table. 
    df = df.join(vaccines_df, on=list(set(df.columns) & set(vaccines_df.columns)), how='outer')
    df = df.join(procedures_df, on=list(set(df.columns) & set(procedures_df.columns)), how='outer')
    df = df.join(devices_df, on=list(set(df.columns) & set(devices_df.columns)), how='outer')
    df = df.join(observations_df, on=list(set(df.columns) & set(observations_df.columns)), how='outer')
    df = df.join(conditions_df, on=list(set(df.columns) & set(conditions_df.columns)), how='outer')
    df = df.join(drugs_df, on=list(set(df.columns) & set(drugs_df.columns)), how='outer')
    df = df.join(measurements_df, on=list(set(df.columns) & set(measurements_df.columns)), how='outer')    
    df = df.join(deaths_df, on=list(set(df.columns) & set(deaths_df.columns)), how='outer')

    # IF we have the lab table, then join it
    if INCLUDE_LAB == True:
        df = df.join(lab_df, on=list(set(df.columns) & set(lab_df.columns)), how='outer')

    # All empty cells are filled with 0 after merging. This is the case for example if a patient had a procedure on a certin visit, but no measurement.
    if INCLUDE_LAB == True:
        df = df.na.fill(value=0, subset = [col for col in df.columns if col not in lab_columns])
    else:
        df = df.na.fill(value=0, subset = [col for col in df.columns if col not in ('BMI_rounded')])
   
    # We want one row per person and visit date. So we group by person_id/date, and take the maximum value of ALL fields (all covariates)
    aggregations = [max(col).alias(col) for col in df.columns if col not in ('person_id','date')]
    df = df.groupby('person_id', 'date').agg(*aggregations)

    # join persons; join visits
    df = persons_df.join(df, 'person_id', 'left')
    df = visits_df.join(df, 'person_id', 'outer')

    # Create a reinfection indicator, minimum 60 day window from index date to subsequent positive test
    reinfection_wait_time = 60
    # First create a filter - person has positive test AND where the difference between the visit date and the first COVID date > 60 days (signifies reinfection)
    condition1 = ( (col('PCR_AG_Pos')==1) & (datediff(col('date'), col('COVID_first_poslab_or_diagnosis_date')) > reinfection_wait_time) )
    # If they fulfil the above conditions, mark 1 in a new column is_reinfection. Filter to those rows; groupby those patients reinfected, and take the earliest date of reinfection
    reinfection_df = df.withColumn('is_reinfection', when(condition1, 1).otherwise(0)).where(col('is_reinfection')==1).groupby('person_id').agg(min('date').alias('date'), max('is_reinfection').alias('is_first_reinfection'))
    # Left join to our main table. 
    df = df.join(reinfection_df, on=['person_id','date'], how='left')

    # Create a 2nd condition: If patient has died AND the death date ['date'] occurred after COVID +test/diagnosis AND the number of days between test/diagnosis and death date ['date'] is less than 60D
    condition2 = (col('COVID_patient_death') ==1) & (datediff("date", "COVID_first_poslab_or_diagnosis_date") > 0) & (datediff("date", "COVID_first_poslab_or_diagnosis_date") < reinfection_wait_time)
    # Then Mark 1 indicating the person died within the 60D window post infection: 'death_within_specified_window_post_covid'
    df = df.withColumn('death_within_specified_window_post_covid', when(condition2, 1).otherwise(0))
        
    # Make a column, marking 1 where the date occurred after the FIRST COVID +test/diagnsos [index]. Repeat marking rows BEFORE COVID index. 
    df = df.withColumn('pre_COVID', when(datediff("COVID_first_poslab_or_diagnosis_date","date")>=0, 1).otherwise(0))
    df = df.withColumn('post_COVID', when(datediff("COVID_first_poslab_or_diagnosis_date","date")<0, 1).otherwise(0))

    # Create 3rd condition: identifying those visits [date] that occur in between the COVID_hospitalization (not ED-visit) start and end dates (from visits_filtered, 1 row per patient)
    condition3 = (datediff("first_COVID_hospitalization_end_date","date")>=0) & (datediff("first_COVID_hospitalization_start_date","date")<=0)
    # Mark those rows with 1. These are the visits during the COVID-related hospitalization (but not the ER-related hospitalization)
    df = df.withColumn('during_first_COVID_hospitalization', when(condition3, 1).otherwise(0))
    
    # We do the same as above, but now identifying visits that occurred ON the COVID-related-ED-visit
    df = df.withColumn('during_first_COVID_ED_visit', when(datediff("first_COVID_ED_only_start_date","date")==0, 1).otherwise(0))

    # drop dates for all facts table once indicators are created for 'during_first_COVID_hospitalization'
    df = df.drop('first_COVID_hospitalization_start_date', 'first_COVID_hospitalization_end_date','first_COVID_ED_only_start_date', 'macrovisit_start_date', 'macrovisit_end_date')

    # Create a table filtering to MACROVISITS (from the macrovisits table - this includes BOTH visits and macrovisits)  
    macrovisits_df = macrovisits_df.select('person_id', 'macrovisit_start_date', 'macrovisit_end_date').where(col('macrovisit_start_date').isNotNull() & col('macrovisit_end_date').isNotNull()).distinct()
        
    # Now - OUTER join macrovisits_df to the main data frame (just person_id and date) on 'person_id'
    # Macrovisits_df has multiple rows per person (each macrovisit). df has multiple rows per person (each visit)
    # EVERY macrovisit row (right table) will be joined to every visit row (left table)
    df_hosp = df.select('person_id', 'date').join(macrovisits_df, on=['person_id'], how= 'outer')
    
    # Create a 4th condition to mark visits within a macrovisit: where the visits occurs before OR AT the end of the macrovisit AND occurs after or ON the start of the macrovisit. 
    # Thus, we are filtering visits occurring WITHIN a macrovisit 
    condition4 = (datediff("macrovisit_end_date", "date") >= 0) & (datediff("macrovisit_start_date", "date") <= 0)
    
    # Now we will create a new column during_macrovisit_hospitalization that marks 1 for all visits occurring within a macrovisit. Only keep rows for visits occurring during macrovisits
    # This will delete duplicate rows (which occurred because of the join) where visits were aligned to macrovisits, and they occurred outside the macrovisit start and end
    # (The correct join to have done here should have been a left join (macrovisit to visit, left joining where the visit occurs BETWEEN macrovisit start and end))
    # df_hosp = df_hosp.withColumn('during_macrovisit_hospitalization', when(condition4, 1).otherwise(0)).drop('macrovisit_start_date', 'macrovisit_end_date').where(col('during_macrovisit_hospitalization') == 1).distinct()

    # Alternative - DO NOT DROP macrovisit_start_date
    keep_macrovisit_end_date = True
    if keep_macrovisit_end_date == True:
        df_hosp = df_hosp.withColumn('during_macrovisit_hospitalization', when(condition4, 1).otherwise(0)).where(col('during_macrovisit_hospitalization') == 1).dropDuplicates(subset = ['person_id','date'])
    else:
        df_hosp = df_hosp.withColumn('during_macrovisit_hospitalization', when(condition4, 1).otherwise(0)).drop('macrovisit_end_date').where(col('during_macrovisit_hospitalization') == 1).dropDuplicates(subset = ['person_id','date'])
    
    # Join the above data frame (columns: person_id, date, during_macrovisit_hospitalization) to our main data frame [which has ALL visits], using a left join on person_id and date
    df = df.join(df_hosp, on=['person_id','date'], how="left")
    
    # final fill of null non-continuous variables with 0
    df = df.na.fill(value=0, subset = [col for col in df.columns if col not in lab_columns])

    ###### NOW - CREATE SEPARATE LISTS OF ESSENTIAL COLUMNS AND THE COVARIATES (ORIGINAL) ##############################
    essential_columns = [
    'person_id',
    'date',
    'first_COVID_ED_only_start_date',
    'first_COVID_hospitalization_start_date',
    'first_COVID_hospitalization_end_date',
    'first_COVID_likely_hospitalization_start_date',
    'first_COVID_likely_hospitalization_end_date',
    'first_COVID_post_likely_hospitalization_start_date',
    'first_COVID_post_likely_hospitalization_end_date',
    'first_COVID_pre_likely_hospitalization_start_date',
    'first_COVID_pre_likely_hospitalization_end_date',
    'COVID_first_PCR_or_AG_lab_positive',
    'COVID_first_diagnosis_date',
    'COVID_first_poslab_or_diagnosis_date',
    'had_vaccine_administered',
    'vaccine_dose_number',

    'PCR_AG_Pos',
    'PCR_AG_Neg',
    'Antibody_Pos',
    'Antibody_Neg',
    'BMI_rounded',
    'COVID_patient_death',
    'is_first_reinfection',
    'death_within_specified_window_post_covid',
    'pre_COVID',
    'post_COVID',
    'three_months_pre_covid_index_date',
    'ten_days_post_covid_index_date',
    'pre_COVID_90',
    'during_COVID',
    'during_first_COVID_hospitalization',
    'during_first_COVID_ED_visit',
    'during_macrovisit_hospitalization',
    'macrovisit_start_date', 
    'macrovisit_end_date',
]

    lab_columns = ["CRP","WBC","LYMPH","ALBUMIN","ALT","EGFR","HEMOGLOBIN","CREATININE","PLT","DIABETES_A1C"]

    # covariates
    covariate_columns = [column for column in df.columns if (column not in essential_columns) & (column not in lab_columns)]

    ########################## NEW SECTION - CREATING NEW COLUMN NAMES AND COMBINE COLUMNS #############################

    ## Primary drug exposure
    df = df.withColumn('paxlovid_treatment', col('PAXLOVID'))

    ########################## NEW SECTION - CREATING NEW COLUMN NAMES AND COMBINE COLUMNS #############################

    ## EXCLUSION - Contraindicated drugs - PRE90
    df = df.withColumn('alpha1_adrenergic_antagonist_exclusion_contraindication_drug', col('ELIG_ALFUZOSIN'))
    df = df.withColumn('anti_arrhythmia_exclusion_contraindication_drug', expr('GREATEST(ELIG_CVDT_DRONEDARONE, ELIG_CVDT_FLECAINIDE, ELIG_ARISCIENCE_DRUG_PROPAFENONE_JA, ELIG_CVDT_QUINIDINE, ELIG_AMIODARONE_ITM, ELIG_CVDT_AMIODARONE, ELIG_CVDT_PROPAFENONE, ELIG_N3C_AMIODARONE_INJECTABLE)'))
    df = df.withColumn('anticonvulsant_exclusion_contraindication_drug', expr('GREATEST(ELIG_ARISCIENCE_DRUG_CARBAMAZEPINE_JA,ELIG_PHENYTOIN,ELIG_PHENOBARBITAL,ELIG_ARISCIENCE_DRUG_PRIMIDONE_JA)'))
    df = df.withColumn('cardiovascular_agents_exclusion_contraindication_drug', expr('GREATEST(ELIG_CVDT_EPLERENONE,ELIG_EPLERENONE,ELIG_ARISCIENCE_DRUG_RANOLAZINE_JA)'))
    df = df.withColumn('anticoagulant_exclusion_contraindication_drug', col('ELIG_ARISCIENCE_DRUG_RIVAROXABAN_JA'))
    df = df.withColumn('beta_adrenoceptor_agonist_exclusion_contraindication_drug', col('ELIG_ARISCIENCE_DRUG_SALMETEROL_JA'))
    df = df.withColumn('pde5_inhibitor_exclusion_contraindication_drug', col('ELIG_ARISCIENCE_DRUG_SILDENAFIL_AD'))
    df = df.withColumn('sedatives_exclusion_contraindication_drug', expr('GREATEST(ELIG_ARISCIENCE_DRUG_TRIAZOLAM_JA,ELIG_DATOS_4CE_SEVERITY_MIDAZOLAM,ELIG_MIDAZOLAM_ITM,MIDAZOLAM)'))
    df = df.withColumn('antigout_exclusion_contraindication_drug', expr('GREATEST(ELIG_COLCHICINE149,ELIG_N3C_COLCHICINE)'))
    df = df.withColumn('antimyobacterial_exclusion_contraindication_drug', col('ELIG_RIFAMPIN144'))
    df = df.withColumn('cf_transmembrane_exclusion_contraindication_drug', col('ELIG_IVACAFTORLUMACAFTOR'))
    df = df.withColumn('migraine_medication_exclusion_contraindication_drug', expr('GREATEST(ELIG_UBROGEPANT,ELIG_ELETRIPTAN)'))
    df = df.withColumn('ergot_derivative_exclusion_contraindication_drug', expr('GREATEST(ELIG_DIHYDROERGOTAMINE,ELIG_ERGOTAMINE)'))
    df = df.withColumn('mineralocorticoid_receptor_antagonist_exclusion_contraindication_drug', col('ELIG_FINERENONE'))
    df = df.withColumn('serotonin_receptor_antgaonist_exclusion_contraindication_drug', col('ELIG_FLIBANSERIN'))
    df = df.withColumn('immunosuppressant_exclusion_contraindication_drug', col('ELIG_ISC_VOCLOSPORIN_ATC_DRUG_CLASS'))
    df = df.withColumn('mttp_inhibitor_exclusion_contraindication_drug', col('ELIG_LOMITAPIDE'))
    df = df.withColumn('antipsychotic_exclusion_contraindication_drug', expr('GREATEST(ELIG_LURASIDONE,ELIG_PIMOZIDE)'))
    df = df.withColumn('opioid_antagonist_exclusion_contraindication_drug', col('ELIG_NALOXEGOL'))
    df = df.withColumn('benign_prostatic_hyperplasia_agent_exclusion_contraindication_drug', col('ELIG_SILODOSIN'))
    df = df.withColumn('vasopressin_receptor_antagonist_exclusion_contraindication_drug', col('ELIG_TOLVAPTAN'))

    ## Exclusion - contraindication conditions - EVER
    df = df.withColumn('severe_kidney_disease_exclusion_contraindication_condition', expr('GREATEST(CKD_STAGE4,CKD_STAGE5)'))
    df = df.withColumn('severe_liver_disease_exclusion_contraindication_condition', col('SEVERE_LIVER_DISEASE'))
    df = df.withColumn('kidney_failure_exclusion_contraindication_condition', expr('GREATEST(DIALYSIS,ESRD,RRT)'))
    # df = df.withColumn('severe_liver_disease_exclusion_contraindication_condition', expr('GREATEST(SEVERE_LIVER_DISEASE,CHILDPUGHCLASSBC)'))

    # Exclusion - current COVID-19 treatments for exclusion - PRE14
    df = df.withColumn('monoclonal_antibodies_outpatient_covid19_treatment', expr('GREATEST(BAMLANIVIMAB,BEBTELOVIMAB,ETESIVIMAB,EVUSHELD,SOTROVIMAB,CASIRIVIMAB_IMDEVIMAB)'))
    df = df.withColumn('antiviral_outpatient_covid19_treatment', expr('GREATEST(REMDISIVIR,MOLNUPIRAVIR)'))
    df = df.withColumn('inpatient_anakinra_baricitinib_tocilizumab_covid19_treatment', expr('GREATEST(BARICITINIB,TOCILIZUMAB,ANAKINRA,PLASMA)'))
    df = df.withColumn('unapproved_hydroxychloroquine_ivermectin_covid19_treatment', expr('GREATEST(HYDROXYCHLOROQUINE,IVERMECTIN)'))

    # Risk factor covariates - EVER
    df = df.withColumn('cancer_risk_factor', expr('GREATEST(ELIG_HEMATOLOGIC_CANCER,ELIG_MALIGNANT_CANCER,ELIG_METASTATIC_SOLID_TUMOR_CANCERS,ELIG_BASAL_CELL_CARCINOMA,MALIGNANTCANCER)'))
    df = df.withColumn('cerebrovascular_disease_risk_factor', expr('GREATEST(ELIG_VSAC_CEREBROVASCULAR_DISEASE_STROKE_OR_TIA,CEREBROVASCULARDISEASE,ELIG_STROKE_ISCHEMIC_OR_HEMORRHAGIC,STROKE,ELIG_VSAC_ISCHEMIC_STROKE_EMBOLISM,ELIG_VSAC_ISCHEMIC_STROKE_OTHER,ELIG_VSAC_ISCHEMIC_STROKE_THROMBOSIS,ELIG_VSAC_CEREBROVASCULAR_DISEASES)'))
    df = df.withColumn('chronic_kidney_disease_risk_factor', expr('GREATEST(ELIG_CHRONIC_KIDNEY_DISEASE,ELIG_CKD_STAGES_13,KIDNEYDISEASE)'))
    df = df.withColumn('liver_disease_risk_factor', expr('GREATEST(MILDLIVERDISEASE,MODERATESEVERELIVERDISEASE)'))
    df = df.withColumn('lung_disease_risk_factor', expr('GREATEST(ELIG_ASTHMA,ELIG_CHRONIC_LUNG_DISEASE,PULMONARYEMBOLISM,COPD)'))
    df = df.withColumn('cystic_fibrosis_risk_factor', col('ELIG_VSAC_CYSTIC_FIBROSIS'))
    df = df.withColumn('dementia_risk_factor', col('ELIG_DEMENTIA'))
    df = df.withColumn('diabetes_mellitus_risk_factor', expr('GREATEST(ELIG_DIABETES_COMPLICATED,ELIG_DIABETES_UNCOMPLICATED)'))
    df = df.withColumn('disability_risk_factor', expr('GREATEST(ELIG_CEREBRAL_PALSY,ELIG_DOWN_SYNDROME,ELIG_RHDT_INATTENTIONATTENTION_DEFICIT,ELIG_DEVELOPMENTAL_DISORDER,ELIG_ARISCIENCE_AUTISM_JA,ELIG_HEMIPLEGIAORPARAPLEGIA,ELIG_SPINAL_CORD_INJURY)'))
    df = df.withColumn('heart_disease_risk_factor', expr('GREATEST(ELIG_CARDIOMYOPATHIES,ELIG_CONGESTIVE_HEART_FAILURE,ELIG_CORONARY_ARTERY_DISEASE,ELIG_CVDT_ATHEROSCLEROSIS,ELIG_MT_ATRIAL_ARRHYTHMIAS,ELIG_MYOCARDIAL_INFARCTION,ELIG_PERIPHERAL_VASCULAR_DISEASE,ELIG_UVA_ATRIAL_FIBRILLATION_OR_FLUTTER,ELIG_VSAC_ATRIAL_FIBRILLATIONFLUTTER,ELIG_VSAC_HEART_FAILURE,ELIG_ACUTE_MYOCARDITIS,HEARTFAILURE_STAGES34)'))
    df = df.withColumn('hemoglobin_blood_disorder_risk_factor', expr('GREATEST(ELIG_SICKLE_CELL_DISEASE,THALASSEMIA)'))
    df = df.withColumn('hiv_risk_factor', expr('GREATEST(ELIG_HIV_INFECTION,HIVINFECTION)'))
    df = df.withColumn('mental_health_disorder_risk_factor', expr('GREATEST(ELIG_ANXIETY,ELIG_DATOS_SNOMED_MENTAL_DISORDER,ELIG_DEPRESSION,ELIG_HCUP_SCHIZOPHRENIA_SPECTRUM_AND_OTHER_PSYCHOTIC_DISORDERS,ELIG_MOOD_DISORDER,ELIG_VSAC_MENTAL_HEALTH_DISORDER,PSYCHOSIS)'))
    df = df.withColumn('obesity_overweight_risk_factor', expr('GREATEST(ELIG_VSAC_OVERWEIGHT_OR_OBESE,OBESITY,ELIG_OBESITY)'))
    df = df.withColumn('immunocompromised_risk_factor', expr('GREATEST(ELIG_AUTOIMMUNE_DISEASEIMMUNODEFICIENCY,ELIG_TRANSPLANT_OF_SOLID_ORGAN_OR_BLOOD_STEM_CELL,OTHERIMMUNOCOMPROMISED)'))
    df = df.withColumn('immunosuppressive_therapy_risk_factor', expr('GREATEST(ELIG_IMMUNOSUPPRESSIVE_THERAPIES,CYCLOSPORINE,SIROLIMUS,TACROLIMUS)'))
    df = df.withColumn('corticosteroid_use_risk_factor', expr('GREATEST(BUDESONIDE,CICLESONIDE,FLUTICASONE,MOMETASONE,SYSTEMICCORTICOSTEROIDS,COVIDREGIMENCORTICOSTEROIDS)'))
    df = df.withColumn('tuberculosis_risk_factor', col('ELIG_TUBERCULOSIS'))
    df = df.withColumn('substance_abuse_risk_factor', col('ELIG_SUBSTANCE_USE_DISORDER'))
    df = df.withColumn('smoking_risk_factor', expr('GREATEST(ELIG_TOBACCO_SMOKER,ELIG_SMOKER_CURRENT)'))

    # Covariate disease interactions (other) - EVER
    df = df.withColumn('hyperlipidemia_covariate_disease_interaction', col('HYPERLIPIDEMIA'))
    df = df.withColumn('hemophilia_covariate_disease_interaction', col('HEMOPHILIA'))

    ## Other conditions and past procedures - EVER
    df = df.withColumn('rheumatologic_disease_condition', col('RHEUMATOLOGICDISEASE'))
    df = df.withColumn('inflammation_condition', expr('GREATEST(INFLAMMATION,LL_MISC)'))
    df = df.withColumn('hypertension_condition', col('HYPERTENSION'))
    df = df.withColumn('pneumonia_condition', expr('GREATEST(LL_PNEUMONIADUETOCOVID,PNEUMONIA)'))
    df = df.withColumn('pregnancy_condition', expr('GREATEST(ELIG_PREGNANT,PREGNANCY)'))

    # Procedure columns - EVER
    df = df.withColumn('supplemental_oxygen_procedure', expr('GREATEST(SUPPLEMENTAL_OXYGEN,HIGH_FLOW_OXYGEN,NON_INVASIVE_VENTILATION)'))
    df = df.withColumn('mechanical_ventilation_ecmo_procedure', expr('GREATEST(INVASIVE_VENTILATION,INVASIVE_MECHANICAL_VENTILATION,MECHANICAL_VENTILATION,ECMO,ECMO_ICU)'))

    # Covariate drug interactions/exposures - EVER
    df = df.withColumn('antipsychotics_covariate_drug_exposure', col('ANTIPSYCHOTICS')) # moderate 
    df = df.withColumn('anti_arrhythmia_covariate_drug_exposure', col('ANTIARRHYTHMIA_DRUGS')) # severe
    df = df.withColumn('anticoagulants_covariate_drug_exposure', col('ANTICOAGULANTS')) # severe
    df = df.withColumn('anti_cancer_covariate_drug_exposure', col('ANTICANCER')) # moderate
    df = df.withColumn('cardiovascular_agents_contraindicated', col('CARDIOVASCULAR_DRUGS')) # severe
    df = df.withColumn('sedatives_covariate_drug_exposure', expr('GREATEST(ELIG_DATOS_4CE_SEVERITY_MIDAZOLAM,ELIG_MIDAZOLAM_ITM,MIDAZOLAM,SEDATIVES)')) # severe
    df = df.withColumn('narcotics_covariate_drug_exposure', expr('GREATEST(FENTANYL,HYDROCODONE,MEPERIDINE,METHADONE,OXYCODONE)')) # severe
    df = df.withColumn('anti_infective_covariate_drug_exposure', expr('GREATEST(ANTIBIOTICS, ANTIMYOBACTERIAL_DRUGS)')) # moderate
    df = df.withColumn('influenza_vaccine_covariate_drug_exposure', col('FLU_VACCINE')) # moderate

    # OTHER Relatively contraindicated drugs - EVER
    df = df.withColumn('antifungal_relatively_relatively_contraindicated', expr('GREATEST(ANTIFUNGALS,ANTIFUNGALS_SYSTEMIC)'))  # moderate
    df = df.withColumn('sgc_stimulator_relatively_contraindicated', col('RIOCIGUAT')) # severe
    df = df.withColumn('alpha1_adrenoceptor_antagonist_relatively_contraindicated', col('TAMSULOSIN')) # severe
    df = df.withColumn('anticonvulsant_relatively_contraindicated', col('CLONAZEPAM')) # moderate
    df = df.withColumn('endothilin_receptor_antagonists_relatively_contraindicated', col('BOSENTAN')) # severe
    df = df.withColumn('neuropsychiatric_agents_relatively_contraindicated', col('NEUROPSYCH_DRUGS')) # moderate
    df = df.withColumn('hepatitis_c_direct_acting_antivirals_relatively_contraindicated', col('HEPATITISC_DRUGS')) 
    df = df.withColumn('antidepressants_relatively_contraindicated', expr('GREATEST(BUPROPION,TRAZODONE)')) # moderate
    df = df.withColumn('jak_inhibitors_relatively_contraindicated', expr('GREATEST(TOFACITINIB,UPADACITINIB)')) # severe
    df = df.withColumn('pde5_inhibitors_relatively_contraindicated', expr('GREATEST(PDE5_INHIBITORS,ELIG_ARISCIENCE_DRUG_SILDENAFIL_AD)')) # severe
    df = df.withColumn('calcium_channel_blockers_relatively_contraindicated', expr('GREATEST(AMLODIPINE,DILTIAZEM,FELODIPINE,VERAPAMIL)')) # moderate
    df = df.withColumn('migraine_medication_relatively_contraindicated', col('RIMEGEPANT')) # severe
    df = df.withColumn('muscarinic_receptor_antagonist_relatively_contraindicated', col('DARIFENACIN')) # severe
    df = df.withColumn('dpp4_inhibitors_relatively_contraindicated', col('SAXAGLIPTIN')) # moderate
    # df = df.withColumn('corticosteroids_relatively_contraindicated', ) # moderate
    # df = df.withColumn('immunosuppressants_relatively_contraindicated', expr('GREATEST(CYCLOSPORINE,SIROLIMUS,TACROLIMUS)')) # severe

    # Vaccination
    df = df.withColumnRenamed('had_vaccine_administered','vaccinated').withColumnRenamed('vaccine_dose_number','vaccinated_dose_count')

    # columns for CCI. We won't use CCI in this version 
    cci_columns = ['ELIG_PERIPHERAL_VASCULAR_DISEASE', 'CEREBROVASCULARDISEASE', 'ELIG_DEMENTIA', 'ELIG_HEMIPLEGIAORPARAPLEGIA', 'ELIG_HIV_INFECTION', 'PEPTICULCER', 'MODERATESEVERELIVERDISEASE', 'MILDLIVERDISEASE', 'ELIG_METASTATIC_SOLID_TUMOR_CANCERS', 'RHEUMATOLOGICDISEASE', 'ELIG_CHRONIC_LUNG_DISEASE', 'ELIG_DIABETES_COMPLICATED', 'ELIG_CHRONIC_KIDNEY_DISEASE', 'ELIG_MALIGNANT_CANCER', 'ELIG_MYOCARDIAL_INFARCTION', 'ELIG_DIABETES_UNCOMPLICATED', 'ELIG_CONGESTIVE_HEART_FAILURE']

    #calculating CCI components BEFORE and up to covid index
    df = df.withColumn('cci_index_covariate_mi', expr('ELIG_MYOCARDIAL_INFARCTION * 1'))
    df = df.withColumn('cci_index_covariate_chf', expr('ELIG_CONGESTIVE_HEART_FAILURE * 1'))
    df = df.withColumn('cci_index_covariate_pvd', expr('ELIG_PERIPHERAL_VASCULAR_DISEASE * 1'))
    df = df.withColumn('cci_index_covariate_cvd', expr('CEREBROVASCULARDISEASE * 1'))
    df = df.withColumn('cci_index_covariate_dem', expr('ELIG_DEMENTIA * 1'))
    df = df.withColumn('cci_index_covariate_cpd', expr('ELIG_CHRONIC_LUNG_DISEASE * 1'))
    df = df.withColumn('cci_index_covariate_rd', expr('RHEUMATOLOGICDISEASE * 1'))
    df = df.withColumn('cci_index_covariate_pep', expr('PEPTICULCER * 1'))
    df = df.withColumn('cci_index_covariate_liv', expr('CASE WHEN MODERATESEVERELIVERDISEASE = 1 THEN MODERATESEVERELIVERDISEASE * 3 WHEN MODERATESEVERELIVERDISEASE = 0 THEN MILDLIVERDISEASE * 1 ELSE 0 END'))
    df = df.withColumn('cci_index_covariate_dia', expr('CASE WHEN ELIG_DIABETES_COMPLICATED = 1 THEN ELIG_DIABETES_COMPLICATED * 2 WHEN ELIG_DIABETES_COMPLICATED = 0 THEN ELIG_DIABETES_UNCOMPLICATED * 1 ELSE 0 END'))
    df = df.withColumn('cci_index_covariate_hem', expr('ELIG_HEMIPLEGIAORPARAPLEGIA * 2'))
    df = df.withColumn('cci_index_covariate_ren', expr('ELIG_CHRONIC_KIDNEY_DISEASE * 2'))
    df = df.withColumn('cci_index_covariate_can', expr('CASE WHEN ELIG_METASTATIC_SOLID_TUMOR_CANCERS = 1 THEN ELIG_METASTATIC_SOLID_TUMOR_CANCERS * 6 WHEN ELIG_METASTATIC_SOLID_TUMOR_CANCERS = 0 THEN ELIG_MALIGNANT_CANCER * 2 ELSE 0 END'))
    df = df.withColumn('cci_index_covariate_hiv', expr('ELIG_HIV_INFECTION * 6'))

    # #calculate CCI score for patients
    # df = df.withColumn('CCI_condition', lit(MI + CHF + PVD + CVD + DEM + CPD + RD + PEP + LIV + DIA + HEM + REN + CAN + HIV).cast(IntegerType()))

    # Drop covariate columns
    df = df.drop(*covariate_columns)
    df = df.drop(*cci_columns)

    # Finally, make sure we filter to our final cohort. 
    df = df.join(persons_df.select('person_id').distinct(), 'person_id', 'inner')

    # Create a first visit column 
    df = df.withColumn('first_visit', expr('MIN(date) OVER(PARTITION BY person_id)'))

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3af1cb46-7f36-4210-8519-70b0a8d54302"),
    cohort_code=Input(rid="ri.foundry.main.dataset.b6450c6b-55d3-41e3-8208-14fa0780cd41"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.900fa2ad-87ea-4285-be30-c6b5bab60e86"),
    customize_concept_sets=Input(rid="ri.foundry.main.dataset.de3b19a8-bb5d-4752-a040-4e338b103af9"),
    vaccines_filtered=Input(rid="ri.foundry.main.dataset.69027164-230c-4bfa-b385-accf3b2b0482")
)
def conditions_filtered(cohort_code, concept_set_members, condition_occurrence, customize_concept_sets, vaccines_filtered):
       
    #bring in only cohort patient ids
    persons = cohort_code.select('person_id')
    
    # Take conditions table - and then join it to our cohort of patients
    conditions_df = condition_occurrence.select('person_id', 'condition_start_date', 'condition_concept_id').where(col('condition_start_date').isNotNull()).withColumnRenamed('condition_start_date','date').withColumnRenamed('condition_concept_id','concept_id').join(persons,'person_id','inner')

    # Take the fusion sheet [customize_concept_sets] - filter to "conditions". Then choose just the concept_set_name and indicator_prefix columns. The indicator_prefix column will be the column name 
    fusion_df = customize_concept_sets.where(col('domain').contains('condition')).select('concept_set_name','indicator_prefix')
    
    #filter concept_set_members [table of concept sets] to only concept ids for the conditions of interest  by INNER joining to the fusion sheet    
    concepts_df = concept_set_members.select('concept_set_name', 'is_most_recent_version', 'concept_id').where(col('is_most_recent_version')=='true').join(fusion_df, 'concept_set_name', 'inner').select('concept_id','indicator_prefix')

    # Filter the conditions table above to only conditions in our concept sets of interest
    # At this point we have multiple rows per patient/condition [indicator_prefix]/date
    df = conditions_df.join(concepts_df, 'concept_id', 'inner')
    
    # Group by patient and date (along the rows), and pivot by condition [along the columns]. The aggregtion will simply be to place a 1 in that cell (if the condition exists for the patient/date)
    df = df.groupby('person_id','date').pivot('indicator_prefix').agg(lit(1)).na.fill(0)
   
    return df
    

#################################################
## Global imports and functions included below ##
#################################################

from pyspark.sql import functions as F

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ff6099cf-f640-4e55-a82b-5d99a236d3f2"),
    expanded_dataset_for_outcome_analysis_matching=Input(rid="ri.foundry.main.dataset.32ef28d9-6ad8-4b41-8a63-c6e4e37036d9")
)
# Cox Regression - Paxlovid - Sequential Trial (64596fdc-a471-4aab-91ad-0bb836f1e552): v1
def coxreg_prep_matching_composite(expanded_dataset_for_outcome_analysis_matching):
    
    outcome = 'composite'
    time_end = 14
    df = expanded_dataset_for_outcome_analysis_matching.select('treatment','person_id','subclass','row_id','{}_time{}'.format(outcome, time_end),'{}{}'.format(outcome, time_end)).where(expr('time = 1'))

    return df
    

#################################################
## Global imports and functions included below ##
#################################################

######## GLOBAL CODE
# We can add this code to the GLOBAL CODE on the RHS pane
# Import the data types we will be using to functions and schemas
from pyspark.sql.types import StructType, DateType, StructField, StringType, IntegerType, FloatType, DoubleType
from pyspark.sql import Row

# Import functions and tools to make functions, and interact with columns with dot functions or SQL functions
from pyspark.sql.functions import lower, upper, col, udf, monotonically_increasing_id, to_date, trim, ltrim, rtrim, avg
from pyspark.sql.functions import length, size, unix_timestamp, from_unixtime, broadcast, to_timestamp, split, when, rand, count, round, countDistinct, product

# Additional Functions
from pyspark.sql.functions import min, max, col, mean, lit, sum, when, regexp_replace, lower, upper, concat_ws, to_date, floor, months_between, datediff, date_add, current_date, least, greatest, last_day, last, expr

# Functions for window functions
from pyspark.sql import Window
from pyspark.sql.functions import row_number

### PYTHON Functions
import datetime as dt

## GLOBAL PY CODE
### Import all necessary packages
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re

# Pandas functions
idx = pd.IndexSlice

# Viewing related functions
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1957ac02-7a23-44b0-8bb1-ecd8fff34c51"),
    expanded_dataset_for_outcome_analysis_matching_compositedeathhosp=Input(rid="ri.foundry.main.dataset.1b4818d5-123b-4f59-a7b8-a341d2330a85")
)
# Cox Regression - Paxlovid - Sequential Trial (64596fdc-a471-4aab-91ad-0bb836f1e552): v1
def coxreg_prep_matching_composite_death_hosp(expanded_dataset_for_outcome_analysis_matching_compositedeathhosp):
    
    outcome = 'composite_death_hosp'
    time_end = 14
    df = expanded_dataset_for_outcome_analysis_matching_compositedeathhosp.select('treatment','person_id','subclass','row_id','{}_time{}'.format(outcome, time_end),'{}{}'.format(outcome, time_end)).where(expr('time = 1'))

    return df
    

#################################################
## Global imports and functions included below ##
#################################################

######## GLOBAL CODE
# We can add this code to the GLOBAL CODE on the RHS pane
# Import the data types we will be using to functions and schemas
from pyspark.sql.types import StructType, DateType, StructField, StringType, IntegerType, FloatType, DoubleType
from pyspark.sql import Row

# Import functions and tools to make functions, and interact with columns with dot functions or SQL functions
from pyspark.sql.functions import lower, upper, col, udf, monotonically_increasing_id, to_date, trim, ltrim, rtrim, avg
from pyspark.sql.functions import length, size, unix_timestamp, from_unixtime, broadcast, to_timestamp, split, when, rand, count, round, countDistinct, product

# Additional Functions
from pyspark.sql.functions import min, max, col, mean, lit, sum, when, regexp_replace, lower, upper, concat_ws, to_date, floor, months_between, datediff, date_add, current_date, least, greatest, last_day, last, expr

# Functions for window functions
from pyspark.sql import Window
from pyspark.sql.functions import row_number

### PYTHON Functions
import datetime as dt

## GLOBAL PY CODE
### Import all necessary packages
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re

# Pandas functions
idx = pd.IndexSlice

# Viewing related functions
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


@transform_pandas(
    Output(rid="ri.foundry.main.dataset.22c99e32-2f92-42c0-b2ec-412442b2c731"),
    expanded_dataset_for_outcome_analysis_matching_deathoutcome=Input(rid="ri.foundry.main.dataset.7fcd678b-62da-48b1-90e6-1c7e11ec5065")
)
# Cox Regression - Paxlovid - Sequential Trial (64596fdc-a471-4aab-91ad-0bb836f1e552): v1
def coxreg_prep_matching_death(expanded_dataset_for_outcome_analysis_matching_deathoutcome):
    
    outcome = 'death'
    time_end = 14
    df = expanded_dataset_for_outcome_analysis_matching_deathoutcome.select('treatment','person_id','subclass','row_id','{}_time{}'.format(outcome, time_end),'{}{}'.format(outcome, time_end)).where(expr('time = 1'))

    return df
    

#################################################
## Global imports and functions included below ##
#################################################

######## GLOBAL CODE
# We can add this code to the GLOBAL CODE on the RHS pane
# Import the data types we will be using to functions and schemas
from pyspark.sql.types import StructType, DateType, StructField, StringType, IntegerType, FloatType, DoubleType
from pyspark.sql import Row

# Import functions and tools to make functions, and interact with columns with dot functions or SQL functions
from pyspark.sql.functions import lower, upper, col, udf, monotonically_increasing_id, to_date, trim, ltrim, rtrim, avg
from pyspark.sql.functions import length, size, unix_timestamp, from_unixtime, broadcast, to_timestamp, split, when, rand, count, round, countDistinct, product

# Additional Functions
from pyspark.sql.functions import min, max, col, mean, lit, sum, when, regexp_replace, lower, upper, concat_ws, to_date, floor, months_between, datediff, date_add, current_date, least, greatest, last_day, last, expr

# Functions for window functions
from pyspark.sql import Window
from pyspark.sql.functions import row_number

### PYTHON Functions
import datetime as dt

## GLOBAL PY CODE
### Import all necessary packages
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re

# Pandas functions
idx = pd.IndexSlice

# Viewing related functions
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


@transform_pandas(
    Output(rid="ri.foundry.main.dataset.edd4f694-66db-4176-bde5-06d64e098f7b"),
    expanded_dataset_for_outcome_analysis_matching_modcomposite=Input(rid="ri.foundry.main.dataset.1a14ee76-280e-46a0-8072-d3adea949e40")
)
# Cox Regression - Paxlovid - Sequential Trial (64596fdc-a471-4aab-91ad-0bb836f1e552): v1
def coxreg_prep_matching_mod_composite(expanded_dataset_for_outcome_analysis_matching_modcomposite):
    
    outcome = 'mod_composite'
    time_end = 14
    df = expanded_dataset_for_outcome_analysis_matching_modcomposite.select('treatment','person_id','subclass','row_id','{}_time{}'.format(outcome, time_end),'{}{}'.format(outcome, time_end)).where(expr('time = 1'))

    return df
    

#################################################
## Global imports and functions included below ##
#################################################

######## GLOBAL CODE
# We can add this code to the GLOBAL CODE on the RHS pane
# Import the data types we will be using to functions and schemas
from pyspark.sql.types import StructType, DateType, StructField, StringType, IntegerType, FloatType, DoubleType
from pyspark.sql import Row

# Import functions and tools to make functions, and interact with columns with dot functions or SQL functions
from pyspark.sql.functions import lower, upper, col, udf, monotonically_increasing_id, to_date, trim, ltrim, rtrim, avg
from pyspark.sql.functions import length, size, unix_timestamp, from_unixtime, broadcast, to_timestamp, split, when, rand, count, round, countDistinct, product

# Additional Functions
from pyspark.sql.functions import min, max, col, mean, lit, sum, when, regexp_replace, lower, upper, concat_ws, to_date, floor, months_between, datediff, date_add, current_date, least, greatest, last_day, last, expr

# Functions for window functions
from pyspark.sql import Window
from pyspark.sql.functions import row_number

### PYTHON Functions
import datetime as dt

## GLOBAL PY CODE
### Import all necessary packages
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re

# Pandas functions
idx = pd.IndexSlice

# Viewing related functions
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


@transform_pandas(
    Output(rid="ri.foundry.main.dataset.de3b19a8-bb5d-4752-a040-4e338b103af9"),
    Paxlovid_Concepts_2025_Concept_Sets=Input(rid="ri.foundry.main.dataset.fc02fd64-ece1-4f7d-94ea-512f8adb1fcd")
)
# customize_concept_sets (62bbffc2-02b2-45ce-87c3-4132c3f9d13a): v7
#The purpose of this node is to optimize the user's experience connecting a customized concept set "fusion sheet" input data frame to replace LL_concept_sets_fusion_SNOMED.

def customize_concept_sets(Paxlovid_Concepts_2025_Concept_Sets):
    input_concept_sets = Paxlovid_Concepts_2025_Concept_Sets
    # df = LL_concept_sets_fusion_SNOMED.select(trim(col('concept_set_name')).alias('concept_set_name'),'indicator_prefix', 'domain', 'pre_during_post')
    df = input_concept_sets.select(trim(col('concept_set_name')).alias('concept_set_name'),'indicator_prefix', 'domain', 'pre_during_post')
    
    return df
    

#################################################
## Global imports and functions included below ##
#################################################

from pyspark.sql import functions as F

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.027c6b7b-88a8-4977-91a8-85e89150ef25"),
    cohort_facts_visit_level_paxlovid=Input(rid="ri.foundry.main.dataset.63c6f325-61d9-4e4f-acc2-2e9e6c301adc"),
    visits_simpler=Input(rid="ri.foundry.main.dataset.e1a48189-26ee-4b58-9160-fa38aaafa5d3")
)
def death_date(cohort_facts_visit_level_paxlovid, visits_simpler):
    visits_simpler = visits_simpler
    cohort_facts_visit_level_covid_out = cohort_facts_visit_level_paxlovid

    df = cohort_facts_visit_level_covid_out.select('person_id','date','COVID_patient_death').where(expr('COVID_patient_death = 1')).withColumnRenamed('date','death_date').select('person_id','death_date')

    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.415dd063-bba0-4954-8f26-e9a4ff5dc77f"),
    Enriched_Death_Table=Input(rid="ri.foundry.main.dataset.b07b1c8f-97a7-43b4-826a-299e456c6e85"),
    cohort_code=Input(rid="ri.foundry.main.dataset.b6450c6b-55d3-41e3-8208-14fa0780cd41"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    death=Input(rid="ri.foundry.main.dataset.d8cc2ad4-215e-4b5d-bc80-80ffb3454875"),
    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.5af2c604-51e0-4afa-b1ae-1e5fa2f4b905"),
    visits_filtered=Input(rid="ri.foundry.main.dataset.09c6fdb9-33f0-4d00-b71d-433cdd927eac")
)
### DEATHS  - ONE ROW PER PATIENT

def deaths_filtered(cohort_code, microvisits_to_macrovisits, concept_set_members, Enriched_Death_Table, visits_filtered, death):
 
    persons = cohort_code.select('person_id', 'data_extraction_date')
    death_df = death \
        .select('person_id', 'death_date') \
        .distinct() \
        .join(persons, on = 'person_id', how = 'inner')
    visits_df = microvisits_to_macrovisits \
        .select('person_id', 'visit_start_date', 'visit_end_date', 'discharge_to_concept_id') \
        .distinct() \
        .join(persons, on = 'person_id', how = 'inner')
    concepts_df = concept_set_members \
        .select('concept_set_name', 'is_most_recent_version', 'concept_id') \
        .where(F.col('is_most_recent_version')=='true')

    #create lists of concept ids to look for in the discharge_to_concept_id column of the visits_df
    death_from_visits_ids = list(concepts_df.where(F.col('concept_set_name') == "DECEASED").select('concept_id').toPandas()['concept_id'])
    hospice_from_visits_ids = list(concepts_df.where(F.col('concept_set_name') == "HOSPICE").select('concept_id').toPandas()['concept_id'])

    #filter visits table to patient and date rows that have DECEASED that matches list of concept_ids
    death_from_visits_df = visits_df \
        .where(F.col('discharge_to_concept_id').isin(death_from_visits_ids)) \
        .drop('discharge_to_concept_id') \
        .distinct()
    #filter visits table to patient rows that have HOSPICE that matches list of concept_ids
    hospice_from_visits_df = visits_df.drop('visit_start_date', 'visit_end_date', 'data_extraction_date') \
        .where(F.col('discharge_to_concept_id').isin(hospice_from_visits_ids)) \
        .drop('discharge_to_concept_id') \
        .distinct()

    #####################################################################
    ###combine relevant visits sourced deaths with deaths table deaths###
    #####################################################################
   
    #keep rows where death_date is plausible 
    #then take earliest recorded date per person
    death_w_dates_df = death_df.where(
        (F.col('death_date') >= "2018-01-01") &
        (F.col('death_date') < (F.col('data_extraction_date')+(365*2)))
    ).groupby('person_id').agg(F.min('death_date').alias('death_date')) \
    .drop('data_extraction_date')

    #from rows of visit table that have concept_id belonging to DECEASED concept set,
    #create new column visit_death_date that is plauisble visit_end_date when available,
    #and plausible visit_start_date when visit_end_date is Null or not plausible
    #then take latest recorded date per person
    death_from_visits_w_dates_df = death_from_visits_df \
        .withColumn('visit_death_date', F.when(
            (F.col('visit_end_date') >= "2018-01-01") &
            (F.col('visit_end_date') < (F.col('data_extraction_date')+(365*2))), F.col('visit_end_date')
            ).when(
            (F.col('visit_start_date') >= "2018-01-01") &
            (F.col('visit_start_date') < (F.col('data_extraction_date')+(365*2))), F.col('visit_start_date')
            ).otherwise(None)
        ).groupby('person_id').agg(F.max('visit_death_date').alias('visit_death_date')) \
        .drop('data_extraction_date')

    #join deaths with dates from both domains
    df = death_w_dates_df.join(death_from_visits_w_dates_df, on = 'person_id', how = 'outer')
    #prioritize death_dates from the deaths table over from visits table
    df = df.withColumn('date', F.when(F.col('death_date').isNotNull(), F.col('death_date')).otherwise(F.col('visit_death_date'))) \
        .drop('death_date', 'visit_death_date')

    #join in patients, without any date, from deaths table
    #join in patients, without any date, from visits table
    #join in patients, without any date, for HOSPICE from visits table
    #inner join with cohort node patients to keep only confirmed covid patients
    df = df.join(death_df.select('person_id'), on='person_id', how='outer') \
        .join(death_from_visits_df.select('person_id'), on='person_id', how='outer') \
        .join(hospice_from_visits_df, on='person_id', how='outer') \
        .join(persons, on = 'person_id', how = 'inner') \
        .dropDuplicates()
    #flag all patients as having died regardless of date
    df = df.withColumn("COVID_patient_death", F.lit(1))
    
    return df

#################################################
## Global imports and functions included below ##
#################################################

from pyspark.sql import functions as F

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a5c60b39-2f97-45d5-a01d-8bf4be84e48a"),
    cohort_code=Input(rid="ri.foundry.main.dataset.b6450c6b-55d3-41e3-8208-14fa0780cd41"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    customize_concept_sets=Input(rid="ri.foundry.main.dataset.de3b19a8-bb5d-4752-a040-4e338b103af9"),
    device_exposure=Input(rid="ri.foundry.main.dataset.d685db48-6583-43d6-8dc5-a9ebae1a827a"),
    procedures_filtered=Input(rid="ri.foundry.main.dataset.1537bb59-51b7-4765-a5b9-e78e6514d8f5")
)
def devices_filtered(device_exposure, cohort_code, concept_set_members, customize_concept_sets, procedures_filtered):

    # Table of our cohort person_ids
    persons = cohort_code.select('person_id')
    
    #filter device exposure table to only cohort patients
    devices_df = device_exposure.select('person_id','device_exposure_start_date','device_concept_id').where(col('device_exposure_start_date').isNotNull()).withColumnRenamed('device_exposure_start_date','date').withColumnRenamed('device_concept_id','concept_id').join(persons,'person_id','inner')

    # filter the fusion sheet for where the domain = devices (currently we have none)
    fusion_df = customize_concept_sets.where(col('domain').contains('device')).select('concept_set_name','indicator_prefix')
    
    # Filter the concept_set_members (concept set table) to our devices of interest in the fusion sheet
    concepts_df = concept_set_members.select('concept_set_name', 'is_most_recent_version', 'concept_id').where(col('is_most_recent_version')=='true').join(fusion_df, 'concept_set_name', 'inner').select('concept_id','indicator_prefix')
        
    # Filter devices to our concept sets (devices) of interest
    df = devices_df.join(concepts_df, 'concept_id', 'inner')
    
    # Pivot the table so that patient/date along rows, and devices along the columns (with 1 if device present NULL otherwise)
    df = df.groupby('person_id','date').pivot('indicator_prefix').agg(lit(1)).na.fill(0)

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.279e6680-9619-4a95-924d-90510ef7a9ab"),
    cohort_code=Input(rid="ri.foundry.main.dataset.b6450c6b-55d3-41e3-8208-14fa0780cd41"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    customize_concept_sets=Input(rid="ri.foundry.main.dataset.de3b19a8-bb5d-4752-a040-4e338b103af9"),
    devices_filtered=Input(rid="ri.foundry.main.dataset.a5c60b39-2f97-45d5-a01d-8bf4be84e48a"),
    drug_exposure=Input(rid="ri.foundry.main.dataset.ec252b05-8f82-4f7f-a227-b3bb9bc578ef")
)
def drugs_filtered(concept_set_members, drug_exposure, cohort_code, customize_concept_sets, devices_filtered):
  
    #bring in only cohort patient ids
    persons = cohort_code.select('person_id')
    
    #filter drug exposure table to only cohort patients    
    drug_df = drug_exposure.select('person_id','drug_exposure_start_date','drug_concept_id').where(col('drug_exposure_start_date').isNotNull()).withColumnRenamed('drug_exposure_start_date','date').withColumnRenamed('drug_concept_id','concept_id').join(persons,'person_id','inner')

    #filter fusion sheet for concept sets and their future variable names that have concepts in the drug domain
    fusion_df = customize_concept_sets.where(col('domain').contains('drug')).select('concept_set_name','indicator_prefix')
    
    #filter concept set members table to only concept ids for the drugs of interest
    concepts_df = concept_set_members.select('concept_set_name', 'is_most_recent_version', 'concept_id').where(col('is_most_recent_version')=='true').join(fusion_df, 'concept_set_name', 'inner').select('concept_id','indicator_prefix')
        
    #find drug exposure information based on matching concept ids for drugs of interest
    df = drug_df.join(concepts_df, on = 'concept_id', how = 'inner')
    
    #collapse to unique person and visit date and pivot on future variable name to create flag for rows associated with the concept sets for drugs of interest
    df = df.groupby('person_id','date').pivot('indicator_prefix').agg(lit(1)).na.fill(0)

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.bb3f1155-74ee-4013-b621-67e08eca5aeb"),
    cohort_code=Input(rid="ri.foundry.main.dataset.b6450c6b-55d3-41e3-8208-14fa0780cd41"),
    cohort_facts_visit_level_paxlovid=Input(rid="ri.foundry.main.dataset.63c6f325-61d9-4e4f-acc2-2e9e6c301adc"),
    long_dataset_all=Input(rid="ri.foundry.main.dataset.fd0ccf46-d321-41d5-a119-2e57940a931d")
)
def eligible_sample_EHR_all(long_dataset_all, cohort_facts_visit_level_paxlovid, cohort_code):
    cohort_facts_visit_level_paxlovid = cohort_facts_visit_level_paxlovid

    cohort_facts_visit_level_covid_out = cohort_facts_visit_level_paxlovid
    treatment_column = 'paxlovid_treatment'
    long_dataset = long_dataset_all
    sample = 'at_risk_severe_covid'
    drop_patients_without_sdoh = True
    drop_patients_without_recent_EGFR = True
    drop_patients_without_BMI = True

    # optional categorizations
    include_variant = True
    categorize_variant = False
    convert_continuous_columns_to_categorical = True
    convert_age_to_categorical = True

    # Limit to top Data Partners or 3 trials
    limit_top_dp = True
    limit_to_3_trials = False
    # top_data_partners = [726,399,569,793,134,376,217,939,655,819,507,102,526,688,207] # version 1
    top_data_partners = [726,569,399,376,819,102,134,526,688,655,939,507,207,770,406] # version 2 - based on new sample; 

    # Calculate CCI
    long_dataset = long_dataset.withColumn('CCI', expr('cci_index_covariate_mi_LVCF + cci_index_covariate_chf_LVCF + cci_index_covariate_pvd_LVCF + cci_index_covariate_cvd_LVCF + cci_index_covariate_dem_LVCF + cci_index_covariate_cpd_LVCF + cci_index_covariate_rd_LVCF + cci_index_covariate_pep_LVCF + cci_index_covariate_liv_LVCF + cci_index_covariate_dia_LVCF + cci_index_covariate_hem_LVCF + cci_index_covariate_ren_LVCF + cci_index_covariate_can_LVCF + cci_index_covariate_hiv_LVCF'))

    long_dataset = long_dataset.withColumnRenamed('sdoh2_by_preferred_county_LVCF','SDOH')
    
    # long_dataset = long_dataset.withColumn('CCI', expr('cci_index_covariate_mi + cci_index_covariate_chf + cci_index_covariate_pvd + cci_index_covariate_cvd + cci_index_covariate_dem + cci_index_covariate_cpd + cci_index_covariate_rd + cci_index_covariate_pep + cci_index_covariate_liv + cci_index_covariate_dia + cci_index_covariate_hem + cci_index_covariate_ren + cci_index_covariate_can + cci_index_covariate_hiv'))

    dropcols = [column for column in long_dataset.columns if 'cci_index_covariate' in column]
    long_dataset = long_dataset.drop(*dropcols)

    # Now we can identify who is eligible
    # We have already removed patients who were treated before COVID
    follow_up_length = 28
    hospitalization_free_period = 10
    include_ED = True
    death_look_forward = 1
    grace_period = 5

    # 1. Remove patients AFTER they have already been treated. For this study, patients are only able to enter a trial in which they were treated. So remove anyone who was not treated with anything
    df = long_dataset.where(expr('day <= {}'.format(grace_period)))
    print('minimal sample', print(df.select(countDistinct(col('person_id'))).toPandas()))
    dropcols = [column for column in df.columns if 'exclusion_contraindication' in column]
    print(dropcols)

    # Remove patients without BMI
    if drop_patients_without_BMI:
        try:
            df = df.where('MISSING_covariate_BMI_LVCF <> 1')
        except:
            None
        print('patient does NOT have missing BMI', print(df.select(countDistinct(col('person_id'))).toPandas()))
    else:
        bmi_columns = [column for column in df.columns if 'covariate_BMI' in column]
        df = df.drop(*bmi_columns)
    

    # Patient has an EGFR measure in the prior 365 days;
    if drop_patients_without_recent_EGFR:
        df = df.where('EGFR_current_LVCF IS NOT NULL')
        print('patient has EGFR measured in the past 12 mo', print(df.select(countDistinct(col('person_id'))).toPandas()))

    # Person is 30 by trial day
    df = df.withColumn('age', expr('DATEDIFF(date, date_of_birth) / 365.25')).where('age >= 18')
    print('Patient is 18+', print(df.select(countDistinct(col('person_id'))).toPandas()))

    # # Exclude patients once they get treated
    df = df.where(expr('treated <= 1'))
    print('patient has not received treatment previously', print(df.select(countDistinct(col('person_id'))).toPandas()))

    #. 2. Remove ALL rows from 1-day before the patient dies
    df = df.where(expr('(death_date IS NULL) OR (date < DATE_ADD(death_date, -{}) ) '.format(death_look_forward)))
    print('patient does not die within 24h of eligibility', print(df.select(countDistinct(col('person_id'))).toPandas()))

    # 2.1 Repeat for hospitalization - remove patients who were hospitalized 10 days before the current trial date
    # That is, keep dates that occur at least 30 days after the most recent hospitalization preCOVID
    df = df.where(expr('(DATE_ADD(date, -{pre_covid_hosp_period}) > hospitalization_date_pre_covid) OR (hospitalization_date_pre_covid IS NULL)'.format(pre_covid_hosp_period = hospitalization_free_period))).drop('hospitalization_date_pre_covid')
    print('patient does not get hospitalized 10 days before COVID', print(df.select(countDistinct(col('person_id'))).toPandas()))
    
    # 2.2 As we did for death, remove all rows from 1-day before the patient is hospitalized. 
    df = df.where(expr('(date < DATE_ADD(hospitalization_date_post_covid, -{}) ) OR (hospitalization_date_post_covid IS NULL)'.format(death_look_forward)))
    print('patient does not get hospitalized within 24h of eligibility', print(df.select(countDistinct(col('person_id'))).toPandas()))

    # 2.1 Repeat for ED visit - remove patients who had an ED visit 30 days before the current trial date
    # That is, keep dates that occur at least 30 days after the most recent hospitalization preCOVID
    if include_ED:
        df = df.where(expr('(DATE_ADD(date, -{pre_covid_hosp_period}) > EDvisit_date_pre_covid) OR (EDvisit_date_pre_covid IS NULL)'.format(pre_covid_hosp_period = hospitalization_free_period))).drop('EDvisit_date_pre_covid')
        
        # 2.2 As we did for death, remove all rows from 1-day before the patient is ED. 
        df = df.where(expr('(date < DATE_ADD(EDvisit_date_post_covid, -{}) ) OR (EDvisit_date_post_covid IS NULL)'.format(death_look_forward)))
        print('patient not ED visit within 24 hours', print(df.select(countDistinct(col('person_id'))).toPandas()))

        ###### NEW CODE Feb 18th
        # 2.3 We will repeat the above for supplemental oxygen; Apply same 10 day restriction 
        df = df.where(expr('(DATE_ADD(date, -{pre_covid_hosp_period}) > oxygen_date_pre_covid) OR (oxygen_date_pre_covid IS NULL)'.format(pre_covid_hosp_period = hospitalization_free_period))).drop('EDvisit_date_pre_covid')
        
        # 2.4 As we did for death, remove all rows from 1-day before the patient is ED. 
        df = df.where(expr('(date < DATE_ADD(oxygen_date_post_covid, -{}) ) OR (oxygen_date_post_covid IS NULL)'.format(death_look_forward)))
        print('patient does not receive supplemental oxygen within 24h', print(df.select(countDistinct(col('person_id'))).toPandas()))

    #. 3. Identify if the patient has at least 30 days of data remaining at each trial. Remove all rows where the patient has < 30 days of data remaining
    df = df.withColumn('remaining_days_of_data', expr('DATEDIFF(end_day, date) + 1')).withColumn('remaining_days_of_data', expr('CASE WHEN remaining_days_of_data > {follow_up} THEN {follow_up} ELSE remaining_days_of_data END'.format(follow_up = follow_up_length)))
    df = df.withColumn('remaining_days_of_data_actual', expr('DATEDIFF(end_day, date) + 1'))
    df = df.where(expr('remaining_days_of_data >= {}'.format(follow_up_length)))
    print('patient has at least 28 days of follow up data', print(df.select(countDistinct(col('person_id'))).toPandas()))

    # 3B. Identify patients with at least 365 days of observation
    df = df.withColumn('observation_period', expr('DATEDIFF(date, first_visit)')).where(expr('observation_period >= 365'))
    print('patient has at least 365 days of pre trial data', print(df.select(countDistinct(col('person_id'))).toPandas()))

    # 5. Limit to data partners with at least 1 ELIGIBLE patient who was treated. 'treatment' is TIME INVARIANT in this node; we will drop this.
    df = df.withColumn('treatment', expr('MAX(treated) OVER(PARTITION BY person_id)'))
    untreated_df = df.select('person_id','treatment','data_partner_id').distinct().groupBy(col('data_partner_id')).agg( (sum(col('treatment')) / countDistinct(col('person_id'))).alias('proportion_treated'),  sum(col('treatment')).alias('number_treated') ).where(expr('number_treated >= 1'))
    df = df.join(untreated_df, on = 'data_partner_id', how = 'inner').drop('treatment','proportion_treated','number_treated')
    print('patient from data partner with at least 1 patient treated', print(df.select(countDistinct(col('person_id'))).toPandas()))

    # 6. Exclude patients based on eligibility criteria
    other_covid_treatment_columns = [column for column in df.columns if ('covid19_treatment' in column) & ('LVCF' in column)]
    contraindication_condition_columns = [column for column in df.columns if ('exclusion_contraindication_condition' in column) & ('LVCF' in column)]
    contraindication_drug_columns = [column for column in df.columns if ('exclusion_contraindication_drug' in column) & ('LVCF' in column)]
       
    # Create strings of the columns in each group
    contraindication_drug_columns_string = ','.join(contraindication_drug_columns)
    contraindication_condition_columns_string = ','.join(contraindication_condition_columns)
    other_covid_treatment_columns_string = ','.join(other_covid_treatment_columns)

    # # APPLY EXCLUSIONS INCREMENTALLY
    # # Indications
    # df = df.withColumn('has_indication', expr('GREATEST({columns})'.format(columns = indication_columns_string))).where(expr('has_indication <> 1'))
    # print('Patient has no indications (conditions) for Diabetes', print(df.select(countDistinct(col('person_id'))).toPandas()))

    # Drug Contraindications
    df = df.withColumn('has_contraindication_drug', expr('GREATEST({columns})'.format(columns = contraindication_drug_columns_string))).where(expr('has_contraindication_drug <> 1'))
    print('Patient has no contraindications drugs', print(df.select(countDistinct(col('person_id'))).toPandas()))

    # Condition Contraindications. This includes EGFR
    df = df.withColumn('has_contraindication_condition', expr('GREATEST({columns})'.format(columns = contraindication_condition_columns_string))).where(expr('has_contraindication_condition <> 1'))
    print('Patient has no contraindications conditions', print(df.select(countDistinct(col('person_id'))).toPandas()))

    # Other COVID tmts
    df = df.withColumn('has_other_covid_tmts', expr('GREATEST({columns})'.format(columns = other_covid_treatment_columns_string))).where(expr('has_other_covid_tmts <> 1'))
    print('Patient not exposed to any other COVID tx', print(df.select(countDistinct(col('person_id'))).toPandas()))

    #### APPLY INCLUSIONS #######
    age_risk_factor = 65
    df = df.withColumn('Age', expr('DATEDIFF(date, date_of_birth)/365.25'))
    df = df.withColumn('age_risk_factor_LVCF', expr('CASE WHEN Age >= {} THEN 1 ELSE 0 END'.format(age_risk_factor)))
    risk_factors_severe_covid = ["age_risk_factor_LVCF","cancer_risk_factor_LVCF","cerebrovascular_disease_risk_factor_LVCF","chronic_kidney_disease_risk_factor_LVCF","liver_disease_risk_factor_LVCF","lung_disease_risk_factor_LVCF","cystic_fibrosis_risk_factor_LVCF","dementia_risk_factor_LVCF","diabetes_mellitus_risk_factor_LVCF","disability_risk_factor_LVCF","heart_disease_risk_factor_LVCF","hemoglobin_blood_disorder_risk_factor_LVCF","hiv_risk_factor_LVCF","mental_health_disorder_risk_factor_LVCF","obesity_overweight_risk_factor_LVCF","immunocompromised_risk_factor_LVCF","immunosuppressive_therapy_risk_factor_LVCF","corticosteroid_use_risk_factor_LVCF","tuberculosis_risk_factor_LVCF","substance_abuse_risk_factor_LVCF","smoking_risk_factor_LVCF"]
    risk_factors_severe_covid_string = ','.join(risk_factors_severe_covid)
    df = df.withColumn('at_risk_severe_covid', expr('GREATEST({})'.format(risk_factors_severe_covid_string)))
    
    if sample == 'at_risk_severe_covid':
        df = df.where(expr('at_risk_severe_covid = 1'))
        print('Patient is at risk of Severe COVID-19', print(df.select(countDistinct(col('person_id'))).toPandas()))
    
    df = df.drop('Age','age_risk_factor_LVCF')

    if drop_patients_without_sdoh == True:
        df = df.dropna(subset = ['SDOH'])
        print('Patient has SDOH index data', print(df.select(countDistinct(col('person_id'))).toPandas()))

    ################# Calculate time to event columns ##############################
    # Rename all date columns
    df = df.withColumnRenamed('hospitalization_date_post_covid','hospitalization_date').withColumnRenamed('EDvisit_date_post_covid','EDvisit_date')
    ###### NEW CODE Feb 18th
    df = df.withColumnRenamed('oxygen_date_post_covid','oxygen_date')

    include_ED = True
    outcome_date_columns = [
        "death_date",
        "hospitalization_date",
    ]

    if include_ED:
        outcome_date_columns += ['EDvisit_date']

        ###### NEW CODE Feb 18th
        outcome_date_columns += ['oxygen_date']
    
    # Create composite column
    df = df.withColumn('composite_date', expr('LEAST({})'.format(','.join(outcome_date_columns))))

    # Begin for death
    df = df.withColumn('death', expr('CASE WHEN death_date IS NOT NULL THEN 1 ELSE 0 END'))
    df = df.withColumn('death_time', expr('CASE WHEN death = 1 THEN DATEDIFF(death_date, date) + 1 ELSE DATEDIFF(end_day, date) + 1 END'))

    df = df.withColumn('death60', expr('CASE WHEN death_date IS NOT NULL AND death_time <= 60 THEN 1 ELSE 0 END'))
    df = df.withColumn('death_time60', expr('CASE WHEN death_time <= 60 THEN death_time ELSE 60 END'))

    df = df.withColumn('death28', expr('CASE WHEN death_date IS NOT NULL AND death_time <= 28 THEN 1 ELSE 0 END'))
    df = df.withColumn('death_time28', expr('CASE WHEN death_time <= 28 THEN death_time ELSE 28 END'))

    df = df.withColumn('death14', expr('CASE WHEN death_date IS NOT NULL AND death_time <= 14 THEN 1 ELSE 0 END'))
    df = df.withColumn('death_time14', expr('CASE WHEN death_time <= 14 THEN death_time ELSE 14 END'))

    # Hospitalization
    df = df.withColumn('hospitalization', expr('CASE WHEN hospitalization_date IS NOT NULL THEN 1 ELSE 0 END'))
    df = df.withColumn('hospitalization_time', expr('CASE WHEN hospitalization = 1 THEN DATEDIFF(hospitalization_date, date) + 1 ELSE DATEDIFF(end_day, date) + 1 END'))

    df = df.withColumn('hospitalization60', expr('CASE WHEN hospitalization_date IS NOT NULL AND hospitalization_time <= 60 THEN 1 ELSE 0 END'))
    df = df.withColumn('hospitalization_time60', expr('CASE WHEN hospitalization_time <= 60 THEN hospitalization_time ELSE 60 END'))

    df = df.withColumn('hospitalization28', expr('CASE WHEN hospitalization_date IS NOT NULL AND hospitalization_time <= 28 THEN 1 ELSE 0 END'))
    df = df.withColumn('hospitalization_time28', expr('CASE WHEN hospitalization_time <= 28 THEN hospitalization_time ELSE 28 END'))

    df = df.withColumn('hospitalization14', expr('CASE WHEN hospitalization_date IS NOT NULL AND hospitalization_time <= 14 THEN 1 ELSE 0 END'))
    df = df.withColumn('hospitalization_time14', expr('CASE WHEN hospitalization_time <= 14 THEN hospitalization_time ELSE 14 END'))

    # # EDvisit
    if include_ED:
        df = df.withColumn('EDvisit', expr('CASE WHEN EDvisit_date IS NOT NULL THEN 1 ELSE 0 END'))
        df = df.withColumn('EDvisit_time', expr('CASE WHEN EDvisit = 1 THEN DATEDIFF(EDvisit_date, date) + 1 ELSE DATEDIFF(end_day, date) + 1 END'))

        df = df.withColumn('EDvisit60', expr('CASE WHEN EDvisit_date IS NOT NULL AND EDvisit_time <= 60 THEN 1 ELSE 0 END'))
        df = df.withColumn('EDvisit_time60', expr('CASE WHEN EDvisit_time <= 60 THEN EDvisit_time ELSE 60 END'))

        df = df.withColumn('EDvisit28', expr('CASE WHEN EDvisit_date IS NOT NULL AND EDvisit_time <= 28 THEN 1 ELSE 0 END'))
        df = df.withColumn('EDvisit_time28', expr('CASE WHEN EDvisit_time <= 28 THEN EDvisit_time ELSE 28 END'))

        df = df.withColumn('EDvisit14', expr('CASE WHEN EDvisit_date IS NOT NULL AND EDvisit_time <= 14 THEN 1 ELSE 0 END'))
        df = df.withColumn('EDvisit_time14', expr('CASE WHEN EDvisit_time <= 14 THEN EDvisit_time ELSE 14 END'))

        ######## NEW CODE - SUPPLEMENTAL OXYGEN
        df = df.withColumn('oxygen', expr('CASE WHEN oxygen_date IS NOT NULL THEN 1 ELSE 0 END'))
        df = df.withColumn('oxygen_time', expr('CASE WHEN oxygen = 1 THEN DATEDIFF(oxygen_date, date) + 1 ELSE DATEDIFF(end_day, date) + 1 END'))

        df = df.withColumn('oxygen60', expr('CASE WHEN oxygen_date IS NOT NULL AND oxygen_time <= 60 THEN 1 ELSE 0 END'))
        df = df.withColumn('oxygen_time60', expr('CASE WHEN oxygen_time <= 60 THEN oxygen_time ELSE 60 END'))

        df = df.withColumn('oxygen28', expr('CASE WHEN oxygen_date IS NOT NULL AND oxygen_time <= 28 THEN 1 ELSE 0 END'))
        df = df.withColumn('oxygen_time28', expr('CASE WHEN oxygen_time <= 28 THEN oxygen_time ELSE 28 END'))

        df = df.withColumn('oxygen14', expr('CASE WHEN oxygen_date IS NOT NULL AND oxygen_time <= 14 THEN 1 ELSE 0 END'))
        df = df.withColumn('oxygen_time14', expr('CASE WHEN oxygen_time <= 14 THEN oxygen_time ELSE 14 END'))

    # Composite
    df = df.withColumn('composite', expr('CASE WHEN composite_date IS NOT NULL THEN 1 ELSE 0 END'))
    df = df.withColumn('composite_time', expr('CASE WHEN composite = 1 THEN DATEDIFF(composite_date, date) + 1 ELSE DATEDIFF(end_day, date) + 1 END'))

    df = df.withColumn('composite60', expr('CASE WHEN composite_date IS NOT NULL AND composite_time <= 60 THEN 1 ELSE 0 END'))
    df = df.withColumn('composite_time60', expr('CASE WHEN composite_time <= 60 THEN composite_time ELSE 60 END'))

    df = df.withColumn('composite28', expr('CASE WHEN composite_date IS NOT NULL AND composite_time <= 28 THEN 1 ELSE 0 END'))
    df = df.withColumn('composite_time28', expr('CASE WHEN composite_time <= 28 THEN composite_time ELSE 28 END'))

    df = df.withColumn('composite14', expr('CASE WHEN composite_date IS NOT NULL AND composite_time <= 14 THEN 1 ELSE 0 END'))
    df = df.withColumn('composite_time14', expr('CASE WHEN composite_time <= 14 THEN composite_time ELSE 14 END'))

    # Create overall treatment column. Treatment is NOW time varying. 
    df = df.withColumn('treatment', expr('CASE WHEN {} = 1 THEN 1 ELSE 0 END'.format(treatment_column)))

    ##################### INCLUDE THE VISITS; AND CONVERT TO DUMMIES ####################################################
    visit_counts = cohort_code.select('person_id','total_ED_visits_before_covid','total_hosp_visits_before_covid','total_visits').fillna(0)
    df = df.join(visit_counts, on = 'person_id', how = 'inner')
    
    # Convert these columns include age into categorical
    intervals = 7
    if convert_continuous_columns_to_categorical: 
        for column in ['total_ED_visits_before_covid','total_hosp_visits_before_covid','total_visits','CCI','SDOH']:
            df = df.withColumn(column, expr('NTILE({}) OVER(ORDER BY {})'.format(intervals, column)))

        # Convert to dummies
        for column in ['total_ED_visits_before_covid','total_hosp_visits_before_covid','total_visits','CCI','SDOH']:
            category_levels = df.select(column).distinct().toPandas()
            category_levels = category_levels[column].tolist()
            category_levels = [cat for cat in category_levels if cat != 1] # don't include level 1
            category_levels_columns = []
            for level in category_levels:
                df = df.withColumn('covariate_{}_{}_LVCF'.format(column, level), expr('CASE WHEN {} = {} THEN 1 ELSE 0 END'.format(column, level)))
                category_levels_columns.append('covariate_{}_{}_LVCF'.format(column, level))
            df = df.drop(column)

    else:

        for column in ['total_ED_visits_before_covid','total_hosp_visits_before_covid','total_visits','CCI','SDOH']:
            df = df.withColumnRenamed(column, 'covariate_{}_LVCF'.format(column))

    # Convert to dummies (do this separate for age, because I don't want to add the 'covariate_' prefix to it)
    if convert_age_to_categorical:
        for column in ['age_at_covid']:
            df = df.withColumn(column, expr('NTILE({}) OVER(ORDER BY {})'.format(intervals, column)))
            category_levels = df.select(column).distinct().toPandas()
            category_levels = category_levels[column].tolist()
            category_levels = [cat for cat in category_levels if cat != 1] # don't include level 1
            category_levels_columns = []
            for level in category_levels:
                df = df.withColumn('{}_{}'.format(column, level), expr('CASE WHEN {} = {} THEN 1 ELSE 0 END'.format(column, level)))
                category_levels_columns.append('{}_{}'.format(column, level))
            df = df.drop(column)

    # # Drop irrelevant indicators; 
    # df = df.drop("covariate_total_ED_visits_before_covid_1_LVCF","covariate_total_hosp_visits_before_covid_1_LVCF","covariate_total_visits_1_LVCF","age_at_covid_1")
    df = df.drop('observation_period_before_covid')

    # Add Variant #
    # Create indicators for COVID-era variant
    #### NEW CODE: ERA: https://www.verywellhealth.com/covid-variants-timeline-6741198#toc-b11529-omicron
    if include_variant:
        df = df.withColumn('variant', expr('CASE \
        WHEN COVID_index_date < "2021-03-01" THEN "PREALPHA" \
        WHEN COVID_index_date < "2021-07-01" THEN "ALPHA" \
        WHEN COVID_index_date < "2021-12-16" THEN "DELTA" \
        WHEN COVID_index_date < "2022-10-01" THEN "OMICRON_BA2" \
        WHEN COVID_index_date < "2023-07-01" THEN "OMICRON_X" \
        WHEN COVID_index_date < "2024-01-01" THEN "OMICRON_E_HV" \
        WHEN COVID_index_date >= "2024-01-01" THEN "OMICRON_LATE" \
        ELSE "OTHER" END'))
    
    variant_columns = []
    if categorize_variant:
        variants = df.select('variant').distinct().toPandas()
        variants = variants['variant'].tolist()
        for variant in variants:
            df = df.withColumn('covariate_variant_{}_LVCF'.format(variant), expr('CASE WHEN variant = "{}" THEN 1 ELSE 0 END'.format(variant)))
            variant_columns.append('covariate_variant_{}_LVCF'.format(variant))
        df = df.drop('variant')

    # Drop all the exclusion contraindication columns we don't need them
    dropcols = [column for column in df.columns if 'exclusion_contraindication' in column]
    df = df.drop(*dropcols)

    ############################
    # Limit to top data partners
    if limit_top_dp:
        df = df.where(col('data_partner_id').isin(top_data_partners))
        print('Top Data Partner - 80% of all treated patients', print(df.select(countDistinct(col('person_id'))).toPandas()))
    if limit_to_3_trials:
        df = df.where(expr('trial <= 3'))
    ############################

    ############################
    if drop_patients_without_sdoh == False:
        # If we keep patients without SDOH then we need to drop the SDOH columns
        sdoh_columns = [column for column in df.columns if 'SDOH' in column]
        df = df.drop(*sdoh_columns)
    
    return df

    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.32ef28d9-6ad8-4b41-8a63-c6e4e37036d9"),
    nearest_neighbor_matching_all=Input(rid="ri.foundry.main.dataset.7ed3e283-f6c6-43f0-baf6-ee29ea81c982"),
    propensity_model_prep_expanded=Input(rid="ri.foundry.main.dataset.9999bcdb-ba34-4487-99b3-64a79b95e279")
)
# Set up expanded data for DTSA (Matching) (d03cbe59-1be7-4e54-bffe-040d39ce84f7): v1
def expanded_dataset_for_outcome_analysis_matching( nearest_neighbor_matching_all, propensity_model_prep_expanded):

    # The purpose of this node is to expand the dataset for discrete time survival analysis; and to set up the dataset for Cox regression; 

    # 1. Create a cross join between person id and days up to day 14; 
    grace_period = 5
    time_end = 14
    follow_up_length = time_end
    outcome = 'composite'
    original_trial_column = 'day'
    outcome_columns = ['{}{}'.format(outcome, time_end), '{}_date'.format(outcome), '{}_time{}'.format(outcome, time_end)]
    outcome_date = '{}_date'.format(outcome)
    weight_column = 'IPTW_ATT'
    
    # Create the cross join
    days = np.arange(1, time_end + grace_period + 1)
    days_df = pd.DataFrame({'time': days})
    days_df = spark.createDataFrame(days_df)
    person_df = nearest_neighbor_matching_all.select('person_id').distinct()
    full_join = person_df.join(days_df)
    
    # 2. Join the matching output to the cross join on person_id only - so that each person gets 30 days
    df = nearest_neighbor_matching_all.select('person_id','day','date','trial','subclass','distance','treatment')
    df = full_join.join(df, on = 'person_id', how = 'inner')

    # 3. Filter out rows (days) that occur before the patient got treated (this is baseline). And then restart time to begin on the trial day. 
    df = df.where(expr('time >= trial'))
    df = df.withColumn('time', expr('ROW_NUMBER() OVER(PARTITION BY person_id, trial ORDER BY date)'))

    # 4. Currently, date is time-invariant, and reflects the trial start date for the patient. Create a time-varying date column by summing date with time (minus 1)
    df = df.withColumn('date', expr('DATE_ADD(date, time) - 1'))
    
    # 5. Get the outcome indicator and date and time value and join to the cross-join dataset. Limit to only rows up to the maximum follow-up length
    outcome = propensity_model_prep_expanded.select(['person_id','day'] + outcome_columns).distinct()
    df = df.join(outcome, on = ['person_id','day'], how = 'inner').where(expr('({} IS NULL) OR (date <= {})'.format(outcome_date, outcome_date)))
    df = df.where(expr('time <= {}'.format(follow_up_length)))
    
    # 6. Create time varying indicator for our selected outcome that turns 1 on the day of the event, and removes rows after it. 
    df = df.withColumn('outcome', expr('CASE WHEN date >= {} THEN 1 ELSE 0 END'.format(outcome_date))).withColumn('outcome', expr('SUM(outcome) OVER(PARTITION BY trial, person_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)')).where(expr('outcome <= 1'))
    
    # 7. Create time indicators, one per day of follow-up; 
    for i in np.arange(1, time_end + 1):
        df = df.withColumn('d{}'.format(i), lit(0))
        df = df.withColumn('d{}'.format(i), expr('CASE WHEN time = {} THEN 1 ELSE d{} END'.format(i, i)))

    # 8. Filter to the final columns we need for analysis
    time_indicator_features = ['d{}'.format(i) for i in np.arange(1, time_end+1)]
    main_columns = ['person_id','time','trial','treatment','subclass']
    target_column = ['outcome']
    final_columns = main_columns + target_column + time_indicator_features + outcome_columns
    
    # 9. Filter the data frame to the final columns we need for analysis. 
    df = df.select(final_columns)

    # Finally - create a row_id variable
    df = df.withColumn('row_id', expr('ROW_NUMBER() OVER(ORDER BY RAND())'))

    # # 10. Join the weights; Weights are time invariant; 
    # weights = propensity_model_all_weights.select('person_id','trial',weight_column)
    # df = df.join(weights, on = ['person_id','trial'], how = 'inner')
    
    return df
    

#################################################
## Global imports and functions included below ##
#################################################

######## GLOBAL CODE
# We can add this code to the GLOBAL CODE on the RHS pane
# Import the data types we will be using to functions and schemas
from pyspark.sql.types import StructType, DateType, StructField, StringType, IntegerType, FloatType, DoubleType
from pyspark.sql import Row

# Import functions and tools to make functions, and interact with columns with dot functions or SQL functions
from pyspark.sql.functions import lower, upper, col, udf, monotonically_increasing_id, to_date, trim, ltrim, rtrim, avg
from pyspark.sql.functions import length, size, unix_timestamp, from_unixtime, broadcast, to_timestamp, split, when, rand, count, round, countDistinct, product

# Additional Functions
from pyspark.sql.functions import min, max, col, mean, lit, sum, when, regexp_replace, lower, upper, concat_ws, to_date, floor, months_between, datediff, date_add, current_date, least, greatest, last_day, last, expr

# Functions for window functions
from pyspark.sql import Window
from pyspark.sql.functions import row_number

### PYTHON Functions
import datetime as dt

## GLOBAL PY CODE
### Import all necessary packages
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re

# Pandas functions
idx = pd.IndexSlice

# Viewing related functions
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1b4818d5-123b-4f59-a7b8-a341d2330a85"),
    nearest_neighbor_matching_all=Input(rid="ri.foundry.main.dataset.7ed3e283-f6c6-43f0-baf6-ee29ea81c982"),
    propensity_model_prep_expanded=Input(rid="ri.foundry.main.dataset.9999bcdb-ba34-4487-99b3-64a79b95e279")
)
# Set up expanded data for DTSA (Matching) (d03cbe59-1be7-4e54-bffe-040d39ce84f7): v1
def expanded_dataset_for_outcome_analysis_matching_compositedeathhosp( nearest_neighbor_matching_all, propensity_model_prep_expanded):

    # The purpose of this node is to expand the dataset for discrete time survival analysis; and to set up the dataset for Cox regression; 

    # 1. Create a cross join between person id and days up to day 14; 
    grace_period = 5
    time_end = 14
    follow_up_length = time_end
    outcome = 'composite_death_hosp'
    original_trial_column = 'day'
    outcome_columns = ['{}{}'.format(outcome, time_end), '{}_date'.format(outcome), '{}_time{}'.format(outcome, time_end)]
    outcome_date = '{}_date'.format(outcome)
    weight_column = 'IPTW_ATT'
    
    # Create the cross join
    days = np.arange(1, time_end + grace_period + 1)
    days_df = pd.DataFrame({'time': days})
    days_df = spark.createDataFrame(days_df)
    person_df = nearest_neighbor_matching_all.select('person_id').distinct()
    full_join = person_df.join(days_df)
    
    # 2. Join the matching output to the cross join on person_id only - so that each person gets 30 days
    df = nearest_neighbor_matching_all.select('person_id','day','date','trial','subclass','distance','treatment')
    df = full_join.join(df, on = 'person_id', how = 'inner')

    # 3. Filter out rows (days) that occur before the patient got treated (this is baseline). And then restart time to begin on the trial day. 
    df = df.where(expr('time >= trial'))
    df = df.withColumn('time', expr('ROW_NUMBER() OVER(PARTITION BY person_id, trial ORDER BY date)'))

    # 4. Currently, date is time-invariant, and reflects the trial start date for the patient. Create a time-varying date column by summing date with time (minus 1)
    df = df.withColumn('date', expr('DATE_ADD(date, time) - 1'))
    
    # 5. Get the outcome indicator and date and time value and join to the cross-join dataset. Limit to only rows up to the maximum follow-up length
    outcome = propensity_model_prep_expanded.select(['person_id','day'] + outcome_columns).distinct()
    df = df.join(outcome, on = ['person_id','day'], how = 'inner').where(expr('({} IS NULL) OR (date <= {})'.format(outcome_date, outcome_date)))
    df = df.where(expr('time <= {}'.format(follow_up_length)))
    
    # 6. Create time varying indicator for our selected outcome that turns 1 on the day of the event, and removes rows after it. 
    df = df.withColumn('outcome', expr('CASE WHEN date >= {} THEN 1 ELSE 0 END'.format(outcome_date))).withColumn('outcome', expr('SUM(outcome) OVER(PARTITION BY trial, person_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)')).where(expr('outcome <= 1'))
    
    # 7. Create time indicators, one per day of follow-up; 
    for i in np.arange(1, time_end + 1):
        df = df.withColumn('d{}'.format(i), lit(0))
        df = df.withColumn('d{}'.format(i), expr('CASE WHEN time = {} THEN 1 ELSE d{} END'.format(i, i)))

    # 8. Filter to the final columns we need for analysis
    time_indicator_features = ['d{}'.format(i) for i in np.arange(1, time_end+1)]
    main_columns = ['person_id','time','trial','treatment','subclass']
    target_column = ['outcome']
    final_columns = main_columns + target_column + time_indicator_features + outcome_columns
    
    # 9. Filter the data frame to the final columns we need for analysis. 
    df = df.select(final_columns)

    # Finally - create a row_id variable
    df = df.withColumn('row_id', expr('ROW_NUMBER() OVER(ORDER BY RAND())'))

    # # 10. Join the weights; Weights are time invariant; 
    # weights = propensity_model_all_weights.select('person_id','trial',weight_column)
    # df = df.join(weights, on = ['person_id','trial'], how = 'inner')
    
    return df
    

#################################################
## Global imports and functions included below ##
#################################################

######## GLOBAL CODE
# We can add this code to the GLOBAL CODE on the RHS pane
# Import the data types we will be using to functions and schemas
from pyspark.sql.types import StructType, DateType, StructField, StringType, IntegerType, FloatType, DoubleType
from pyspark.sql import Row

# Import functions and tools to make functions, and interact with columns with dot functions or SQL functions
from pyspark.sql.functions import lower, upper, col, udf, monotonically_increasing_id, to_date, trim, ltrim, rtrim, avg
from pyspark.sql.functions import length, size, unix_timestamp, from_unixtime, broadcast, to_timestamp, split, when, rand, count, round, countDistinct, product

# Additional Functions
from pyspark.sql.functions import min, max, col, mean, lit, sum, when, regexp_replace, lower, upper, concat_ws, to_date, floor, months_between, datediff, date_add, current_date, least, greatest, last_day, last, expr

# Functions for window functions
from pyspark.sql import Window
from pyspark.sql.functions import row_number

### PYTHON Functions
import datetime as dt

## GLOBAL PY CODE
### Import all necessary packages
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re

# Pandas functions
idx = pd.IndexSlice

# Viewing related functions
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.7fcd678b-62da-48b1-90e6-1c7e11ec5065"),
    nearest_neighbor_matching_all=Input(rid="ri.foundry.main.dataset.7ed3e283-f6c6-43f0-baf6-ee29ea81c982"),
    propensity_model_prep_expanded=Input(rid="ri.foundry.main.dataset.9999bcdb-ba34-4487-99b3-64a79b95e279")
)
# Set up expanded data for DTSA (Matching) (d03cbe59-1be7-4e54-bffe-040d39ce84f7): v1
def expanded_dataset_for_outcome_analysis_matching_deathoutcome( nearest_neighbor_matching_all, propensity_model_prep_expanded):

    # The purpose of this node is to expand the dataset for discrete time survival analysis; and to set up the dataset for Cox regression; 

    # 1. Create a cross join between person id and days up to day 14; 
    grace_period = 5
    time_end = 14
    follow_up_length = time_end
    outcome = 'death'
    original_trial_column = 'day'
    outcome_columns = ['{}{}'.format(outcome, time_end), '{}_date'.format(outcome), '{}_time{}'.format(outcome, time_end)]
    outcome_date = '{}_date'.format(outcome)
    weight_column = 'IPTW_ATT'
    
    # Create the cross join
    days = np.arange(1, time_end + grace_period + 1)
    days_df = pd.DataFrame({'time': days})
    days_df = spark.createDataFrame(days_df)
    person_df = nearest_neighbor_matching_all.select('person_id').distinct()
    full_join = person_df.join(days_df)
    
    # 2. Join the matching output to the cross join on person_id only - so that each person gets 30 days
    df = nearest_neighbor_matching_all.select('person_id','day','date','trial','subclass','distance','treatment')
    df = full_join.join(df, on = 'person_id', how = 'inner')

    # 3. Filter out rows (days) that occur before the patient got treated (this is baseline). And then restart time to begin on the trial day. 
    df = df.where(expr('time >= trial'))
    df = df.withColumn('time', expr('ROW_NUMBER() OVER(PARTITION BY person_id, trial ORDER BY date)'))

    # 4. Currently, date is time-invariant, and reflects the trial start date for the patient. Create a time-varying date column by summing date with time (minus 1)
    df = df.withColumn('date', expr('DATE_ADD(date, time) - 1'))
    
    # 5. Get the outcome indicator and date and time value and join to the cross-join dataset. Limit to only rows up to the maximum follow-up length
    outcome = propensity_model_prep_expanded.select(['person_id','day'] + outcome_columns).distinct()
    df = df.join(outcome, on = ['person_id','day'], how = 'inner').where(expr('({} IS NULL) OR (date <= {})'.format(outcome_date, outcome_date)))
    df = df.where(expr('time <= {}'.format(follow_up_length)))
    
    # 6. Create time varying indicator for our selected outcome that turns 1 on the day of the event, and removes rows after it. 
    df = df.withColumn('outcome', expr('CASE WHEN date >= {} THEN 1 ELSE 0 END'.format(outcome_date))).withColumn('outcome', expr('SUM(outcome) OVER(PARTITION BY trial, person_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)')).where(expr('outcome <= 1'))
    
    # 7. Create time indicators, one per day of follow-up; 
    for i in np.arange(1, time_end + 1):
        df = df.withColumn('d{}'.format(i), lit(0))
        df = df.withColumn('d{}'.format(i), expr('CASE WHEN time = {} THEN 1 ELSE d{} END'.format(i, i)))

    # 8. Filter to the final columns we need for analysis
    time_indicator_features = ['d{}'.format(i) for i in np.arange(1, time_end+1)]
    main_columns = ['person_id','time','trial','treatment','subclass']
    target_column = ['outcome']
    final_columns = main_columns + target_column + time_indicator_features + outcome_columns
    
    # 9. Filter the data frame to the final columns we need for analysis. 
    df = df.select(final_columns)

    # Finally - create a row_id variable
    df = df.withColumn('row_id', expr('ROW_NUMBER() OVER(ORDER BY RAND())'))

    # # 10. Join the weights; Weights are time invariant; 
    # weights = propensity_model_all_weights.select('person_id','trial',weight_column)
    # df = df.join(weights, on = ['person_id','trial'], how = 'inner')
    
    return df
    

#################################################
## Global imports and functions included below ##
#################################################

######## GLOBAL CODE
# We can add this code to the GLOBAL CODE on the RHS pane
# Import the data types we will be using to functions and schemas
from pyspark.sql.types import StructType, DateType, StructField, StringType, IntegerType, FloatType, DoubleType
from pyspark.sql import Row

# Import functions and tools to make functions, and interact with columns with dot functions or SQL functions
from pyspark.sql.functions import lower, upper, col, udf, monotonically_increasing_id, to_date, trim, ltrim, rtrim, avg
from pyspark.sql.functions import length, size, unix_timestamp, from_unixtime, broadcast, to_timestamp, split, when, rand, count, round, countDistinct, product

# Additional Functions
from pyspark.sql.functions import min, max, col, mean, lit, sum, when, regexp_replace, lower, upper, concat_ws, to_date, floor, months_between, datediff, date_add, current_date, least, greatest, last_day, last, expr

# Functions for window functions
from pyspark.sql import Window
from pyspark.sql.functions import row_number

### PYTHON Functions
import datetime as dt

## GLOBAL PY CODE
### Import all necessary packages
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re

# Pandas functions
idx = pd.IndexSlice

# Viewing related functions
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1a14ee76-280e-46a0-8072-d3adea949e40"),
    nearest_neighbor_matching_all=Input(rid="ri.foundry.main.dataset.7ed3e283-f6c6-43f0-baf6-ee29ea81c982"),
    propensity_model_prep_expanded=Input(rid="ri.foundry.main.dataset.9999bcdb-ba34-4487-99b3-64a79b95e279")
)
# Set up expanded data for DTSA (Matching) (d03cbe59-1be7-4e54-bffe-040d39ce84f7): v1
def expanded_dataset_for_outcome_analysis_matching_modcomposite( nearest_neighbor_matching_all, propensity_model_prep_expanded):

    # The purpose of this node is to expand the dataset for discrete time survival analysis; and to set up the dataset for Cox regression; 

    # 1. Create a cross join between person id and days up to day 14; 
    grace_period = 5
    time_end = 14
    follow_up_length = time_end
    outcome = 'mod_composite'
    original_trial_column = 'day'
    outcome_columns = ['{}{}'.format(outcome, time_end), '{}_date'.format(outcome), '{}_time{}'.format(outcome, time_end)]
    outcome_date = '{}_date'.format(outcome)
    weight_column = 'IPTW_ATT'
    
    # Create the cross join
    days = np.arange(1, time_end + grace_period + 1)
    days_df = pd.DataFrame({'time': days})
    days_df = spark.createDataFrame(days_df)
    person_df = nearest_neighbor_matching_all.select('person_id').distinct()
    full_join = person_df.join(days_df)
    
    # 2. Join the matching output to the cross join on person_id only - so that each person gets 30 days
    df = nearest_neighbor_matching_all.select('person_id','day','date','trial','subclass','distance','treatment')
    df = full_join.join(df, on = 'person_id', how = 'inner')

    # 3. Filter out rows (days) that occur before the patient got treated (this is baseline). And then restart time to begin on the trial day. 
    df = df.where(expr('time >= trial'))
    df = df.withColumn('time', expr('ROW_NUMBER() OVER(PARTITION BY person_id, trial ORDER BY date)'))

    # 4. Currently, date is time-invariant, and reflects the trial start date for the patient. Create a time-varying date column by summing date with time (minus 1)
    df = df.withColumn('date', expr('DATE_ADD(date, time) - 1'))
    
    # 5. Get the outcome indicator and date and time value and join to the cross-join dataset. Limit to only rows up to the maximum follow-up length
    outcome = propensity_model_prep_expanded.select(['person_id','day'] + outcome_columns).distinct()
    df = df.join(outcome, on = ['person_id','day'], how = 'inner').where(expr('({} IS NULL) OR (date <= {})'.format(outcome_date, outcome_date)))
    df = df.where(expr('time <= {}'.format(follow_up_length)))
    
    # 6. Create time varying indicator for our selected outcome that turns 1 on the day of the event, and removes rows after it. 
    df = df.withColumn('outcome', expr('CASE WHEN date >= {} THEN 1 ELSE 0 END'.format(outcome_date))).withColumn('outcome', expr('SUM(outcome) OVER(PARTITION BY trial, person_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)')).where(expr('outcome <= 1'))
    
    # 7. Create time indicators, one per day of follow-up; 
    for i in np.arange(1, time_end + 1):
        df = df.withColumn('d{}'.format(i), lit(0))
        df = df.withColumn('d{}'.format(i), expr('CASE WHEN time = {} THEN 1 ELSE d{} END'.format(i, i)))

    # 8. Filter to the final columns we need for analysis
    time_indicator_features = ['d{}'.format(i) for i in np.arange(1, time_end+1)]
    main_columns = ['person_id','time','trial','treatment','subclass']
    target_column = ['outcome']
    final_columns = main_columns + target_column + time_indicator_features + outcome_columns
    
    # 9. Filter the data frame to the final columns we need for analysis. 
    df = df.select(final_columns)

    # Finally - create a row_id variable
    df = df.withColumn('row_id', expr('ROW_NUMBER() OVER(ORDER BY RAND())'))

    # # 10. Join the weights; Weights are time invariant; 
    # weights = propensity_model_all_weights.select('person_id','trial',weight_column)
    # df = df.join(weights, on = ['person_id','trial'], how = 'inner')
    
    return df
    

#################################################
## Global imports and functions included below ##
#################################################

######## GLOBAL CODE
# We can add this code to the GLOBAL CODE on the RHS pane
# Import the data types we will be using to functions and schemas
from pyspark.sql.types import StructType, DateType, StructField, StringType, IntegerType, FloatType, DoubleType
from pyspark.sql import Row

# Import functions and tools to make functions, and interact with columns with dot functions or SQL functions
from pyspark.sql.functions import lower, upper, col, udf, monotonically_increasing_id, to_date, trim, ltrim, rtrim, avg
from pyspark.sql.functions import length, size, unix_timestamp, from_unixtime, broadcast, to_timestamp, split, when, rand, count, round, countDistinct, product

# Additional Functions
from pyspark.sql.functions import min, max, col, mean, lit, sum, when, regexp_replace, lower, upper, concat_ws, to_date, floor, months_between, datediff, date_add, current_date, least, greatest, last_day, last, expr

# Functions for window functions
from pyspark.sql import Window
from pyspark.sql.functions import row_number

### PYTHON Functions
import datetime as dt

## GLOBAL PY CODE
### Import all necessary packages
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re

# Pandas functions
idx = pd.IndexSlice

# Viewing related functions
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.afa7d1bd-aa4c-4b84-a3de-304af864c759"),
    propensity_model_prep_expanded=Input(rid="ri.foundry.main.dataset.9999bcdb-ba34-4487-99b3-64a79b95e279")
)
def full_matching_prep(propensity_model_prep_expanded):
    
    """
    This node prepares the dataset for full matching using a discrete time survival model
    The final output should include: person_id, day(trial), data_partner, treatment indicator, covariates, outcome, time indicators. 
    In addition to: time*predictor interactions; 

    """
    df = propensity_model_prep_expanded

    # Set up variables for time-dependent interactions
    time_dependent_covariates = False
    multiply_covariates_linear_time = True

    # Limit the time indicators
    grace_period = 5

    # Set up variables
    id_variable = 'person_id'
    target_variable = 'treatment'
    treatment = 'treatment'
    time_variable = 'day'
    
    # include the data partner as indicators?
    include_dp = False

    # Do we want to limit to top 80% of data partners
    limit_top_dp = True

    # Do we want to limit to trials 1-3 only
    limit_to_3_trials = True

    # Set up predictors
    import re
    covariate_columns = [column for column in df.columns if (('covariate_' in column) | ('condition' in column) | ('procedure' in column) | ('vaccinated' in column) | ('risk_factor' in column)) & ('LVCF' in column) & ('exclusion' not in column)]
    demographics = [column for column in df.columns if ('race_ethnicity' in column) | ('age_at_covid' in column)] + ['female']

    bmi_columns = [
        "UNDERWEIGHT_LVCF",
        "NORMAL_LVCF",
        "OVERWEIGHT_LVCF",
        "OBESE_LVCF",
        "OBESE_CLASS_3_LVCF",
        "MISSING_BMI_LVCF",
    ]
    
    outcome_columns = [
        "death_date"
        ,"death"
        ,"death_time"
        ,"death60"
        ,"death_time60"
        ,"death28"
        ,"death_time28"
        ,"death14"
        ,"death_time14"
        ,"hospitalization_date"
        ,"hospitalization"
        ,"hospitalization_time"
        ,"hospitalization60"
        ,"hospitalization_time60"
        ,"hospitalization28"
        ,"hospitalization_time28"
        ,"hospitalization14"
        ,"hospitalization_time14"
        ,"EDvisit_date"
        ,"EDvisit"
        ,"EDvisit_time"
        ,"EDvisit60"
        ,"EDvisit_time60"
        ,"EDvisit28"
        ,"EDvisit_time28"
        ,"EDvisit14"
        ,"EDvisit_time14"
        ,"composite_date"
        ,"composite"
        ,"composite_time"
        ,"composite60"
        ,"composite_time60"
        ,"composite28"
        ,"composite_time28"
        ,"composite14"
        ,"composite_time14"
        ]

    # Create final list of predictors
    full_predictors = [column for column in covariate_columns + demographics + bmi_columns if column in df.columns]

    ############## SET UP PROPENSITY MODEL PREDICTORS AND CALCULATE TIME INDICATORS PLUS INTERACTIONS ######################################################
    # Create time indicators
    time_indicator_columns = []
    for i in np.arange(1, grace_period + 1):
        df = df.withColumn('d{}'.format(i), expr('CASE WHEN day = {day} THEN 1 ELSE 0 END'.format(day = i)))
        time_indicator_columns.append('d{}'.format(i))

    # Create interactions between each time indicator and each predictor
    time_dependent_interaction_columns = []
    if time_dependent_covariates:
        if multiply_covariates_linear_time:
            for predictor in full_predictors:
                df = df.withColumn('{}_x_{}'.format(time_variable, predictor), expr('{} * {}'.format(time_indicator, predictor)))
                time_dependent_interaction_columns.append('{}_x_{}'.format(time_indicator, predictor))
        else:
            for time_indicator in time_indicator_columns:
                for predictor in full_predictors:
                    df = df.withColumn('{}_x_{}'.format(time_indicator, predictor), expr('{} * {}'.format(time_indicator, predictor)))
                    time_dependent_interaction_columns.append('{}_x_{}'.format(time_indicator, predictor))

    # Data partners - if we want, we can create dummy variables for data partners' and then multiply these with time (indicators OR the day column)
    data_partner_columns = []
    data_partner_interactions = []
    if include_dp:
        gps = df.select('data_partner_id').distinct().toPandas()
        gps = gps['data_partner_id'].tolist()
        
        for i in gps:
            df = df.withColumn('data_partner_id_{}'.format(i), expr('CASE WHEN data_partner_id = "{}" THEN 1 ELSE 0 END'.format(i)))
            data_partner_columns.append('data_partner_id_{}'.format(i))

        # Multiply each data partner indicator with day
        for column in data_partner_columns:
            df = df.withColumn(column + '_x_day', expr('{} * day'.format(column)))
            data_partner_interactions.append(column + '_x_day')
        
    # Get the final analysis columns. Excluded 'trial', 'date' for time being. 
    # If there are no time dependent covariates: we just need the time indicators and the predictors
    if (time_dependent_covariates == False):
        analysis_columns = [id_variable] + ['data_partner_id'] + [target_variable] + [time_variable] + time_indicator_columns + full_predictors
    # If there are interactions with linear time and predictors, include time indicators, features, and the feature*time interactions
    elif (time_dependent_covariates == True) & (multiply_covariates_linear_time == True): 
        analysis_columns = [id_variable] + ['data_partner_id'] + [target_variable] + [time_variable] + time_indicator_columns + full_predictors + time_dependent_interaction_columns
    # Uf there are interactions with time indicators and predictors, include time indicators and the feature*time_indicator interactions
    elif (time_dependent_covariates == True) & (multiply_covariates_linear_time == False): 
        analysis_columns = [id_variable] + ['data_partner_id'] + [target_variable] + [time_variable] + time_indicator_columns + time_dependent_interaction_columns
    
    if include_dp:
        analysis_columns += data_partner_columns + data_partner_interactions

    # Filter our dataset to those columns 
    df = df.select(analysis_columns)

    ############################
    # Limit to top data partners
    if limit_top_dp:
        df = df.where(col('data_partner_id').isin([726,399,569,793,134,376,217,939,655,819,507,102,526,688,207]))
    if limit_to_3_trials:
        df = df.where(expr('trial <= 3'))
    ############################

    # Finally rename all covariates to have the suffix '_COVARIATE'
    if (time_dependent_covariates == False):
        final_covariates = time_indicator_columns + full_predictors
    
    elif (time_dependent_covariates == True) & (multiply_covariates_linear_time == True): 
        final_covariates = time_indicator_columns + full_predictors + time_dependent_interaction_columns
    
    elif (time_dependent_covariates == True) & (multiply_covariates_linear_time == False): 
        final_covariates = time_indicator_columns + time_dependent_interaction_columns

    for column in final_covariates:
        df = df.withColumnRenamed(column, column + "_COVARIATE")
    
    # return final
    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.65ffc6be-73f1-431d-b720-29da7cbcc433"),
    death_date=Input(rid="ri.foundry.main.dataset.027c6b7b-88a8-4977-91a8-85e89150ef25"),
    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.5af2c604-51e0-4afa-b1ae-1e5fa2f4b905"),
    minimal_patient_cohort=Input(rid="ri.foundry.main.dataset.9b0b693d-1e29-4eeb-aa02-a86f1c7c7fb6")
)
def hospitalizations(microvisits_to_macrovisits, minimal_patient_cohort, death_date):
    minimal_patient_cohort = minimal_patient_cohort

    # This code will create a data frame of all the hospitalizations the patient has experienced
    # For the outcome, we will need to exclude patients hospitalized 30 days before the trial date
    # AND we will need to exclude patients who are hospitalized within 24 hours of entering the trial

    hospitalizations = microvisits_to_macrovisits.selectExpr('person_id','visit_start_date AS date','macrovisit_start_date','likely_hospitalization').where('likely_hospitalization = 1').withColumn('hospitalized', lit(1)).join(minimal_patient_cohort.select('person_id','COVID_first_poslab_or_diagnosis_date').distinct(), on = 'person_id', how = 'inner').orderBy('person_id','date')

    # We should end up with 1 row per patient with the following columns: FIRST hospitalization on/after COVID-19; MOST RECENT hospitalization date 30 days before COVID-19
    result = hospitalizations.withColumn('pre_post_covid', expr('CASE WHEN date < COVID_first_poslab_or_diagnosis_date THEN 1 WHEN date >= COVID_first_poslab_or_diagnosis_date THEN 2 ELSE NULL END'))
    result = result.withColumn('hospitalization_date_post_covid', expr('MIN(CASE WHEN pre_post_covid = 2 THEN date ELSE NULL END) OVER(PARTITION BY person_id)'))
    result = result.withColumn('hospitalization_date_30_days_pre_covid', expr('MAX(CASE WHEN date BETWEEN DATE_ADD(COVID_first_poslab_or_diagnosis_date, -31) AND DATE_ADD(COVID_first_poslab_or_diagnosis_date, -1) THEN date ELSE NULL END) OVER(PARTITION BY person_id)'))
    result = result.withColumn('earliest_hospitalization_date', expr('MIN(date) OVER(PARTITION BY person_id)'))
    result = result.withColumn('hospitalization_date_pre_covid', expr('MAX(CASE WHEN pre_post_covid = 1 THEN date ELSE NULL END) OVER(PARTITION BY person_id)'))

    return result.select('person_id','hospitalization_date_post_covid','hospitalization_date_pre_covid').distinct().where(expr('NOT(hospitalization_date_post_covid IS NULL AND hospitalization_date_pre_covid IS NULL)'))

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.350d80cb-4977-435a-a21c-894648e6daf1"),
    cohort_code=Input(rid="ri.foundry.main.dataset.b6450c6b-55d3-41e3-8208-14fa0780cd41"),
    cohort_facts_visit_level_paxlovid=Input(rid="ri.foundry.main.dataset.63c6f325-61d9-4e4f-acc2-2e9e6c301adc"),
    long_covid_date=Input(rid="ri.foundry.main.dataset.69dade5f-c43f-457a-89da-652a3e4a6e2a"),
    long_covid_date_v2=Input(rid="ri.foundry.main.dataset.5f0de766-9252-47b9-b981-c43c93a1181d"),
    long_dataset_all=Input(rid="ri.foundry.main.dataset.fd0ccf46-d321-41d5-a119-2e57940a931d"),
    paxlovid_bootstrap_DTSA_death=Input(rid="ri.foundry.main.dataset.3993bec5-34a6-47d9-b910-6698d1c77119")
)
def long_covid_sample(long_dataset_all, cohort_facts_visit_level_paxlovid, cohort_code, long_covid_date, long_covid_date_v2, paxlovid_bootstrap_DTSA_death):
    
    cohort_facts_visit_level_paxlovid = cohort_facts_visit_level_paxlovid

    cohort_facts_visit_level_covid_out = cohort_facts_visit_level_paxlovid
    treatment_column = 'paxlovid_treatment'
    long_dataset = long_dataset_all
    sample = 'at_risk_severe_covid'
    drop_patients_without_BMI = True
    drop_patients_without_recent_EGFR= True
    drop_patients_without_sdoh = True
    age_risk_cutoff = 65
    
    # Limit to top Data Partners or 3 trials
    limit_top_dp = True
    limit_to_3_trials = False
    top_data_partners = [726,569,399,376,819,102,134,526,688,655,939,507,207,770,406] 

    # Calculate CCI
    long_dataset = long_dataset.withColumn('CCI', expr('cci_index_covariate_mi_LVCF + cci_index_covariate_chf_LVCF + cci_index_covariate_pvd_LVCF + cci_index_covariate_cvd_LVCF + cci_index_covariate_dem_LVCF + cci_index_covariate_cpd_LVCF + cci_index_covariate_rd_LVCF + cci_index_covariate_pep_LVCF + cci_index_covariate_liv_LVCF + cci_index_covariate_dia_LVCF + cci_index_covariate_hem_LVCF + cci_index_covariate_ren_LVCF + cci_index_covariate_can_LVCF + cci_index_covariate_hiv_LVCF'))

    long_dataset = long_dataset.withColumnRenamed('sdoh2_by_preferred_county_LVCF','SDOH')
    
    # long_dataset = long_dataset.withColumn('CCI', expr('cci_index_covariate_mi + cci_index_covariate_chf + cci_index_covariate_pvd + cci_index_covariate_cvd + cci_index_covariate_dem + cci_index_covariate_cpd + cci_index_covariate_rd + cci_index_covariate_pep + cci_index_covariate_liv + cci_index_covariate_dia + cci_index_covariate_hem + cci_index_covariate_ren + cci_index_covariate_can + cci_index_covariate_hiv'))

    dropcols = [column for column in long_dataset.columns if 'cci_index_covariate' in column]
    long_dataset = long_dataset.drop(*dropcols)

    # Now we can identify who is eligible
    # We have already removed patients who were treated before COVID
    follow_up_length = 28
    hospitalization_free_period = 10
    include_ED = True
    death_look_forward = 1
    grace_period = 5

    # 1. Remove patients AFTER they have already been treated. For this study, patients are only able to enter a trial in which they were treated. So remove anyone who was not treated with anything
    df = long_dataset.where(expr('day <= {}'.format(grace_period)))
    print('minimal sample', print(df.select(countDistinct(col('person_id'))).toPandas()))
    dropcols = [column for column in df.columns if 'exclusion_contraindication' in column]
    print(dropcols)

    # Remove patients without BMI
    if drop_patients_without_BMI: # Drop Patients Missing BMI
        df = df.where('MISSING_covariate_BMI_LVCF <> 1')
        print('patient does NOT have missing BMI', print(df.select(countDistinct(col('person_id'))).toPandas()))
    else: # Keep patients with missing BMI
        bmi_columns = [column for column in df.columns if 'covariate_BMI' in column]
        df = df.drop(*bmi_columns)

    # Patient has an EGFR measure in the prior 365 days;
    if drop_patients_without_recent_EGFR:
        df = df.where('EGFR_current_LVCF IS NOT NULL')
        print('patient has EGFR measured in the past 12 mo', print(df.select(countDistinct(col('person_id'))).toPandas()))

    # Person is 30 by trial day
    df = df.withColumn('age', expr('DATEDIFF(date, date_of_birth) / 365.25')).where('age >= 18')
    print('Patient is 18+', print(df.select(countDistinct(col('person_id'))).toPandas()))

    # # Exclude patients once they get treated
    df = df.where(expr('treated <= 1'))
    print('patient has not received treatment previously', print(df.select(countDistinct(col('person_id'))).toPandas()))

    #. 2. Remove ALL rows from 1-day before the patient dies
    df = df.where(expr('(death_date IS NULL) OR (date < DATE_ADD(death_date, -{}) ) '.format(death_look_forward)))
    print('patient does not die within 24h of eligibility', print(df.select(countDistinct(col('person_id'))).toPandas()))

    # 2.1 Repeat for hospitalization - remove patients who were hospitalized 10 days before the current trial date
    # That is, keep dates that occur at least 30 days after the most recent hospitalization preCOVID
    df = df.where(expr('(DATE_ADD(date, -{pre_covid_hosp_period}) > hospitalization_date_pre_covid) OR (hospitalization_date_pre_covid IS NULL)'.format(pre_covid_hosp_period = hospitalization_free_period))).drop('hospitalization_date_pre_covid')
    print('patient does not get hospitalized 10 days before COVID', print(df.select(countDistinct(col('person_id'))).toPandas()))
    
    # 2.2 As we did for death, remove all rows from 1-day before the patient is hospitalized. 
    df = df.where(expr('(date < DATE_ADD(hospitalization_date_post_covid, -{}) ) OR (hospitalization_date_post_covid IS NULL)'.format(death_look_forward)))
    print('patient does not get hospitalized within 24h of eligibility', print(df.select(countDistinct(col('person_id'))).toPandas()))

    # 2.1 Repeat for ED visit - remove patients who had an ED visit 30 days before the current trial date
    # That is, keep dates that occur at least 30 days after the most recent hospitalization preCOVID
    if include_ED:
        df = df.where(expr('(DATE_ADD(date, -{pre_covid_hosp_period}) > EDvisit_date_pre_covid) OR (EDvisit_date_pre_covid IS NULL)'.format(pre_covid_hosp_period = hospitalization_free_period))).drop('EDvisit_date_pre_covid')
        
        # 2.2 As we did for death, remove all rows from 1-day before the patient is ED. 
        df = df.where(expr('(date < DATE_ADD(EDvisit_date_post_covid, -{}) ) OR (EDvisit_date_post_covid IS NULL)'.format(death_look_forward)))
        print('patient not ED visit within 24 hours', print(df.select(countDistinct(col('person_id'))).toPandas()))

        ###### NEW CODE Feb 18th
        # 2.3 We will repeat the above for supplemental oxygen; Apply same 10 day restriction 
        df = df.where(expr('(DATE_ADD(date, -{pre_covid_hosp_period}) > oxygen_date_pre_covid) OR (oxygen_date_pre_covid IS NULL)'.format(pre_covid_hosp_period = hospitalization_free_period))).drop('EDvisit_date_pre_covid')
        
        # 2.4 As we did for death, remove all rows from 1-day before the patient is ED. 
        df = df.where(expr('(date < DATE_ADD(oxygen_date_post_covid, -{}) ) OR (oxygen_date_post_covid IS NULL)'.format(death_look_forward)))
        print('patient does not receive supplemental oxygen within 24h', print(df.select(countDistinct(col('person_id'))).toPandas()))

    # 2.5 Exclude patients who had LONG COVID before their COVID index date
    df = df.join(long_covid_date_v2.withColumnRenamed('date','long_covid_date').withColumnRenamed('LONG_COVID','long_covid').select('person_id','long_covid_date','long_covid','window_start'), on = 'person_id', how = 'left')
    # If we want to use the START of the long covid prediction window as the event time, then set the below to True; 
    use_start_of_window = True
    if use_start_of_window:
        df = df.withColumn('LC_event_date', expr('CASE WHEN window_start IS NOT NULL THEN window_start ELSE long_covid_date END'))
    else:
        df = df.withColumn('LC_event_date', expr('long_covid_date'))
    # Now, we exclude patients who had LC before their index date
    df = df.where(expr('( LC_event_date >= COVID_index_date ) OR ( LC_event_date IS NULL )'.format(death_look_forward)))
    print('No Long COVID before index', print(df.select(countDistinct(col('person_id'))).toPandas()))

    #. 3. Identify if the patient has at least 30 days of data remaining at each trial. Remove all rows where the patient has < 30 days of data remaining
    df = df.withColumn('remaining_days_of_data', expr('DATEDIFF(end_day, date) + 1')).withColumn('remaining_days_of_data', expr('CASE WHEN remaining_days_of_data > {follow_up} THEN {follow_up} ELSE remaining_days_of_data END'.format(follow_up = follow_up_length)))
    df = df.withColumn('remaining_days_of_data_actual', expr('DATEDIFF(end_day, date) + 1'))
    df = df.where(expr('remaining_days_of_data >= {}'.format(follow_up_length)))
    print('patient has at least 28 days of follow up data', print(df.select(countDistinct(col('person_id'))).toPandas()))

    # 3B. Identify patients with at least 365 days of observation
    df = df.withColumn('observation_period', expr('DATEDIFF(date, first_visit)')).where(expr('observation_period >= 365'))
    print('patient has at least 365 days of pre trial data', print(df.select(countDistinct(col('person_id'))).toPandas()))

    # 5. Limit to data partners with at least 1 ELIGIBLE patient who was treated. 'treatment' is TIME INVARIANT;
    df = df.withColumn('treatment', expr('MAX(treated) OVER(PARTITION BY person_id)'))
    untreated_df = df.select('person_id','treatment','data_partner_id').distinct().groupBy(col('data_partner_id')).agg( (sum(col('treatment')) / countDistinct(col('person_id'))).alias('proportion_treated'),  sum(col('treatment')).alias('number_treated') ).where(expr('number_treated >= 1'))
    df = df.join(untreated_df, on = 'data_partner_id', how = 'inner').drop('treatment','proportion_treated','number_treated')
    print('patient from data partner with at least 1 patient treated', print(df.select(countDistinct(col('person_id'))).toPandas()))

    # 6. Exclude patients based on eligibility criteria
    other_covid_treatment_columns = [column for column in df.columns if ('covid19_treatment' in column) & ('LVCF' in column)]
    contraindication_condition_columns = [column for column in df.columns if ('exclusion_contraindication_condition' in column) & ('LVCF' in column)]
    contraindication_drug_columns = [column for column in df.columns if ('exclusion_contraindication_drug' in column) & ('LVCF' in column)]
       
    # Create strings of the columns in each group
    contraindication_drug_columns_string = ','.join(contraindication_drug_columns)
    contraindication_condition_columns_string = ','.join(contraindication_condition_columns)
    other_covid_treatment_columns_string = ','.join(other_covid_treatment_columns)

    # # APPLY EXCLUSIONS INCREMENTALLY
    # # Indications
    # df = df.withColumn('has_indication', expr('GREATEST({columns})'.format(columns = indication_columns_string))).where(expr('has_indication <> 1'))
    # print('Patient has no indications (conditions) for Diabetes', print(df.select(countDistinct(col('person_id'))).toPandas()))

    # Drug Contraindications
    df = df.withColumn('has_contraindication_drug', expr('GREATEST({columns})'.format(columns = contraindication_drug_columns_string))).where(expr('has_contraindication_drug <> 1'))
    print('Patient has no contraindications drugs', print(df.select(countDistinct(col('person_id'))).toPandas()))

    # Condition Contraindications
    df = df.withColumn('has_contraindication_condition', expr('GREATEST({columns})'.format(columns = contraindication_condition_columns_string))).where(expr('has_contraindication_condition <> 1'))
    print('Patient has no contraindications conditions', print(df.select(countDistinct(col('person_id'))).toPandas()))

    # Other COVID tmts
    df = df.withColumn('has_other_covid_tmts', expr('GREATEST({columns})'.format(columns = other_covid_treatment_columns_string))).where(expr('has_other_covid_tmts <> 1'))
    print('Patient not exposed to any other COVID tx', print(df.select(countDistinct(col('person_id'))).toPandas()))

    #### APPLY INCLUSIONS #######
    df = df.withColumn('Age', expr('DATEDIFF(date, date_of_birth)/365.25'))
    df = df.withColumn('age_risk_factor_LVCF', expr('CASE WHEN Age >= {} THEN 1 ELSE 0 END'.format(age_risk_cutoff)))
    risk_factors_severe_covid = ["age_risk_factor_LVCF","cancer_risk_factor_LVCF","cerebrovascular_disease_risk_factor_LVCF","chronic_kidney_disease_risk_factor_LVCF","liver_disease_risk_factor_LVCF","lung_disease_risk_factor_LVCF","cystic_fibrosis_risk_factor_LVCF","dementia_risk_factor_LVCF","diabetes_mellitus_risk_factor_LVCF","disability_risk_factor_LVCF","heart_disease_risk_factor_LVCF","hemoglobin_blood_disorder_risk_factor_LVCF","hiv_risk_factor_LVCF","mental_health_disorder_risk_factor_LVCF","obesity_overweight_risk_factor_LVCF","immunocompromised_risk_factor_LVCF","immunosuppressive_therapy_risk_factor_LVCF","corticosteroid_use_risk_factor_LVCF","tuberculosis_risk_factor_LVCF","substance_abuse_risk_factor_LVCF","smoking_risk_factor_LVCF"]
    risk_factors_severe_covid_string = ','.join(risk_factors_severe_covid)
    df = df.withColumn('at_risk_severe_covid', expr('GREATEST({})'.format(risk_factors_severe_covid_string)))
    
    if sample == 'at_risk_severe_covid':
        df = df.where(expr('at_risk_severe_covid = 1'))
        print('Patient is at risk of Severe COVID-19', print(df.select(countDistinct(col('person_id'))).toPandas()))
    
    df = df.drop('Age','age_risk_factor_LVCF')

    if drop_patients_without_sdoh == True:
        df = df.dropna(subset = ['SDOH'])
        print('Patient has SDOH index data', print(df.select(countDistinct(col('person_id'))).toPandas()))

    ############################
    # Limit to top data partners
    if limit_top_dp:
        df = df.where(col('data_partner_id').isin(top_data_partners))
        print('Top Data Partner - 80% of all treated patients', print(df.select(countDistinct(col('person_id'))).toPandas()))
    if limit_to_3_trials:
        df = df.where(expr('trial <= 3'))
    ############################

    ############################
    if drop_patients_without_sdoh == False:
        # If we keep patients without SDOH then we need to drop the SDOH columns
        sdoh_columns = [column for column in df.columns if 'SDOH' in column]
        df = df.drop(*sdoh_columns)

  
    return df.select('person_id','day')

    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.fd0ccf46-d321-41d5-a119-2e57940a931d"),
    ED_visits=Input(rid="ri.foundry.main.dataset.0ef602f8-450d-43f1-a28f-b0dc7947143a"),
    baseline_variables_stacked=Input(rid="ri.foundry.main.dataset.e910efc8-0261-40da-86f1-2f90e2d5e44b"),
    cohort_facts_visit_level_paxlovid=Input(rid="ri.foundry.main.dataset.63c6f325-61d9-4e4f-acc2-2e9e6c301adc"),
    death_date=Input(rid="ri.foundry.main.dataset.027c6b7b-88a8-4977-91a8-85e89150ef25"),
    hospitalizations=Input(rid="ri.foundry.main.dataset.65ffc6be-73f1-431d-b720-29da7cbcc433"),
    minimal_patient_cohort=Input(rid="ri.foundry.main.dataset.9b0b693d-1e29-4eeb-aa02-a86f1c7c7fb6"),
    oxygen_date=Input(rid="ri.foundry.main.dataset.91a6e009-64ca-466c-b3ba-34b2a086b9f8"),
    person_date_cross_join=Input(rid="ri.foundry.main.dataset.20a8907e-c506-4116-a04c-ad96436639f8")
)
def long_dataset_all(minimal_patient_cohort, cohort_facts_visit_level_paxlovid, hospitalizations, person_date_cross_join, ED_visits, death_date, baseline_variables_stacked, oxygen_date):
    person_date_cross_join = person_date_cross_join
    minimal_patient_cohort = minimal_patient_cohort
    cohort_facts_visit_level_covid_out = cohort_facts_visit_level_paxlovid

    # # Treatment list #################################################################################
    treatment_column = 'paxlovid_treatment'
    treatment_exposures = [treatment_column]
    grace_period_length = 5
    treatments = ','.join(treatment_exposures)
    complete_case = False # Baseline Variables Missing
    complete_case_columns = ['BMI_rounded','EGFR','CREATININE','ALT','sdoh2_by_preferred_county']
    ######################################################################################################

    # Set up all facts table
    all_facts = cohort_facts_visit_level_covid_out

    # Begin with the cross join dataset; We only need days 1-5 (from COVID index). Also include the 6th day
    df = person_date_cross_join.withColumnRenamed('time','day').where(expr('day <= {} + 1'.format(grace_period_length)))
    print('Minimal Patient Sample', print(df.select(countDistinct(col('person_id'))).toPandas()))

    # 1. From the minimal patient cohort table, obtain the time invariant demographic variables. 
    details = minimal_patient_cohort.drop(*["PAXLOVID","METFORMIN","FLUVOXAMINE","IVERMECTIN","MONTELUKAST","ALBUTEROL","AZITHROMYCIN","FLUTICASONE","exposed"]).withColumnRenamed('COVID_first_poslab_or_diagnosis_date','COVID_index_date')    

    # 2. Limit the minimal patient cohort details table to only patients who were never treated PRIOR the grace period. 
    never_treated = baseline_variables_stacked.selectExpr('person_id', '{} AS previously_treated'.format(treatment_column)).where(expr('previously_treated = 0')).select('person_id').distinct()
    details = details.join(never_treated, on = 'person_id', how = 'inner')

    # Merge the details (1 row per patient) to the cross join table as a one-to-many join
    df = df.join(details, on = 'person_id', how = 'inner')
    print('Patients never Treated with study drug prior to a trial occurring during the grace period', print(df.select(countDistinct(col('person_id'))).toPandas()))

    # 3. Next join the baseline variables for all 7 trials to our cross join table. First identify the covariates in the baseline table
    time_varying_covariates = [column for column in baseline_variables_stacked.columns if column not in ['person_id','date','number_of_visits','observation_period']]
    time_varying_covariates = [column for column in time_varying_covariates if column in baseline_variables_stacked.columns]
    print(time_varying_covariates)
    lab_values = ["CRP","WBC","LYMPH","ALBUMIN","ALT","EGFR","DIABETES_A1C","HEMOGLOBIN","CREATININE","PLT"]
    
    # 4. Join the baseline_variables to our cross join table (with demographics)
    df = df.join(baseline_variables_stacked.select(['person_id','date'] + time_varying_covariates), on = ['person_id', 'date'], how = 'inner')

    # 4.1 Optional to drop patients for complete case analysis
    if complete_case:
        df = df.dropna(subset = complete_case_columns)
    print('After keeping complete cases', print(df.select(countDistinct(col('person_id'))).toPandas()))

    # 5. Rename ALL the time varying covariates to have the _LVCF extension
    for column in time_varying_covariates:
        df = df.withColumnRenamed(column, column + '_LVCF')

    # 5. From the all facts table, extract the same time-varying covariates (current values). Exclude BMI and vaccinated. Join to our cross join table.
    time_varying_covariates_modified = [column for column in time_varying_covariates if column in all_facts.columns]
    all_facts_sub = all_facts.select(['person_id', 'date'] + time_varying_covariates_modified).drop('vaccinated','BMI_rounded')
    df = df.join(all_facts_sub, on = ['person_id','date'], how = 'left')

    # 7. Categorize the BMI_LVCF column. We will treat this as the time-varying BMI column. 
    ##### categorize BMI and make into dummy ######
    df = df.withColumn('BMI_rounded_LVCF', expr('CASE WHEN BMI_rounded_LVCF = 0 THEN NULL ELSE BMI_rounded_LVCF END'))
    df = df.withColumn('bmi_category_LVCF', expr("CASE WHEN BMI_rounded_LVCF < 25 THEN 'NORMAL' \
    WHEN BMI_rounded_LVCF >= 25 AND BMI_rounded_LVCF < 30 THEN 'OVERWEIGHT' \
    WHEN BMI_rounded_LVCF >= 30 AND BMI_rounded_LVCF < 40 THEN 'OBESE' \
    WHEN BMI_rounded_LVCF >= 40 THEN 'OBESE_CLASS_3' \
    ELSE 'MISSING' END")).drop('BMI_rounded_LVCF','BMI_rounded')

    # 8. Convert to dummy; add these as columns
    bmi_columns = []
    gps = df.select('bmi_category_LVCF').distinct().toPandas()
    gps = gps['bmi_category_LVCF'].tolist()
    for bmi_cat in gps:
        df = df.withColumn('{}_covariate_BMI_LVCF'.format(bmi_cat), expr('CASE WHEN bmi_category_LVCF = "{}" THEN 1 ELSE 0 END'.format(bmi_cat)))
        bmi_columns.append('{}_LVCF'.format(bmi_cat))
    df = df.drop('bmi_category_LVCF')
    time_varying_covariates += bmi_columns

    # 9. Create dummy variables for the demographics - race and gender
    df = df.withColumn('race_ethnicity_white', when(col('race_ethnicity') == 'White Non-Hispanic', 1).otherwise(0))
    df = df.withColumn('race_ethnicity_hispanic_latino', when(col('race_ethnicity') == 'Hispanic or Latino Any Race', 1).otherwise(0))
    df = df.withColumn('race_ethnicity_black', when(col('race_ethnicity') == 'Black or African American Non-Hispanic', 1).otherwise(0))
    df = df.withColumn('race_ethnicity_asian', when(col('race_ethnicity') == 'Asian Non-Hispanic', 1).otherwise(0))
    df = df.withColumn('race_ethnicity_aian', when(col('race_ethnicity') == 'American Indian or Alaska Native Non-Hispanic', 1).otherwise(0))
    df = df.withColumn('race_ethnicity_nhpi', when(col('race_ethnicity') == 'Native Hawaiian or Other Pacific Islander Non-Hispanic', 1).otherwise(0))
    # df = df.withColumn('race_ethnicity_other', expr("CASE WHEN race_ethnicity IN ('Other Non-Hispanic','Unknown') THEN 1 ELSE 0 END"))
    df = df.drop('race_ethnicity')

    # 10. For all time varying covariates except LAB values, we will fill with 0's including the treatment exposures
    columns_to_fill = [column for column in time_varying_covariates if column not in lab_values]
    columns_to_fill = [column for column in columns_to_fill if column in df.columns]
    for column in columns_to_fill:
        df = df.withColumn(column, expr('NVL({}, 0)'.format(column)))

    # 11. Perform two cumulative sums of the treatment exposure columns so we can identify when patients were treated for the first time. Include a column that indicate ANY treatment (treated). AT THIS POINT WE ARE NOT EXCLUDING PATIENTS ONCE TREATED; 
    df = df.withColumn('treated', col('{}'.format(treatment_column)))
    
    # treatment_exposures_mod = treatment_exposures + ['treated']
    treatment_exposures_mod = ['treated']
    for column in treatment_exposures_mod:
        df = df.withColumn(column, expr('SUM({treatment}) OVER(PARTITION BY person_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)'.format(treatment = column))).withColumn(column, expr('SUM({treatment}) OVER(PARTITION BY person_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)'.format(treatment = column)))

    # Join in the hospitalization, death, and ED visit dates ### NEW CODE Feb 18th
    df = df.join(death_date, on = 'person_id', how = 'left').join(hospitalizations, on = 'person_id', how = 'left').join(ED_visits, on = 'person_id', how = 'left').join(oxygen_date, on = 'person_id', how = 'left')

    # Add a variable to define first visit so we can exclude patients with less than 365 days of observation prior to COVID19
    first_visit = all_facts.select('first_visit','person_id').distinct()
    df = df.join(first_visit, on = 'person_id', how = 'inner')

    # Create a variable identifying the last day of data for each patient. Some patients die after data extraction. This is actually the LAST date of observation for these patients
    df = df.withColumn('end_day', expr('GREATEST(data_extraction_date, death_date)'))
    df = df.withColumn('end_day', expr('LEAST(end_day, CURRENT_DATE())'))

    # Create Sex column
    df = df.withColumn('female', expr('CASE WHEN sex = "FEMALE" THEN 1 ELSE 0 END')).drop('sex')

    # # 10. Add in the lab variables - categorize these. First create variable for exclusion based on EGFRR. This is based only on the most recent value
    # df = df.withColumn('exclusion_contraindication_condition_EGFR_LVCF', expr('CASE WHEN EGFR_LVCF > 0 AND EGFR_LVCF < 30 THEN 1 ELSE 0 END'))
    df = df.withColumn('exclusion_contraindication_condition_EGFR_LVCF', expr('CASE WHEN EGFR_LVCF >= 0 AND EGFR_LVCF < 30 THEN 1 ELSE 0 END'))

    # 11. Now categorize the lab measures and create dummy variables. The low cutoff has been changed to 90 for EGFR
    lab_columns = ["CRP_LVCF","WBC_LVCF","LYMPH_LVCF","ALBUMIN_LVCF","ALT_LVCF","EGFR_LVCF","PLT_LVCF","CREATININE_LVCF","HEMOGLOBIN_LVCF","DIABETES_A1C_LVCF"]
    df = df.withColumn('CRP_LVCF', expr('CASE WHEN CRP_LVCF > 10 THEN 2 WHEN CRP_LVCF IS NOT NULL THEN 1 ELSE NULL END'))
    df = df.withColumn('WBC_LVCF', expr('CASE WHEN WBC_LVCF = 0 THEN NULL WHEN WBC_LVCF < 4500 THEN 2 WHEN WBC_LVCF > 11000 THEN 3 WHEN WBC_LVCF IS NOT NULL THEN 1 ELSE NULL END'))
    df = df.withColumn('LYMPH_LVCF', expr('CASE WHEN LYMPH_LVCF = 0 THEN NULL WHEN LYMPH_LVCF > 4800 THEN 2 WHEN LYMPH_LVCF < 1000 THEN 3 WHEN LYMPH_LVCF IS NOT NULL THEN 1 ELSE NULL END'))
    df = df.withColumn('ALBUMIN_LVCF', expr('CASE WHEN ALBUMIN_LVCF = 0 THEN NULL WHEN ALBUMIN_LVCF < 3.4 THEN 2 WHEN ALBUMIN_LVCF > 5.4 THEN 3 WHEN ALBUMIN_LVCF IS NOT NULL THEN 1 ELSE NULL END')) 
    df = df.withColumn('ALT_LVCF', expr('CASE WHEN ALT_LVCF < 7 THEN 2 WHEN ALT_LVCF > 56 THEN 3 WHEN ALT_LVCF IS NOT NULL THEN 1 ELSE NULL END'))
    df = df.withColumn('PLT_LVCF', expr('CASE WHEN PLT_LVCF = 0 THEN NULL WHEN PLT_LVCF < 150000 THEN 2 WHEN PLT_LVCF > 450000 THEN 3 WHEN PLT_LVCF IS NOT NULL THEN 1 ELSE NULL END'))
    df = df.withColumn('CREATININE_LVCF', expr('CASE WHEN CREATININE_LVCF = 0 THEN NULL WHEN CREATININE_LVCF < 0.5 THEN 2 WHEN CREATININE_LVCF > 7.5 THEN 3 WHEN CREATININE_LVCF IS NOT NULL THEN 1 ELSE NULL END'))
    df = df.withColumn('HEMOGLOBIN_LVCF', expr('CASE WHEN HEMOGLOBIN_LVCF = 0 THEN NULL WHEN HEMOGLOBIN_LVCF < 12 AND female = 1 THEN 2 WHEN HEMOGLOBIN_LVCF < 13 AND female = 0 THEN 2 WHEN HEMOGLOBIN_LVCF > 16 AND female = 1 THEN 3 WHEN HEMOGLOBIN_LVCF > 17 AND female = 0 THEN 3 WHEN HEMOGLOBIN_LVCF IS NOT NULL THEN 1 ELSE NULL END'))
    df = df.withColumn('DIABETES_A1C_LVCF_CONTINUOUS', col('DIABETES_A1C_LVCF'))
    df = df.withColumn('DIABETES_A1C_LVCF', expr('CASE WHEN DIABETES_A1C_LVCF = 0 THEN NULL WHEN DIABETES_A1C_LVCF < 5.7 THEN 2 WHEN DIABETES_A1C_LVCF IS NOT NULL THEN 1 ELSE NULL END'))
    df = df.withColumn('EGFR_LVCF', expr('CASE WHEN EGFR_LVCF = 0 THEN NULL WHEN EGFR_LVCF < 90 THEN 2 WHEN EGFR_LVCF IS NOT NULL THEN 1 ELSE NULL END'))

    # # Create dummy variables for the lab measures
    # df = df.withColumn('crp_covariate_high_LVCF', expr('CASE WHEN CRP_LVCF = 2 THEN 1 ELSE 0 END'))
    # df = df.withColumn('crp_covariate_normal_LVCF', expr('CASE WHEN CRP_LVCF = 1 THEN 1 ELSE 0 END'))
    # df = df.withColumn('wbc_covariate_high_LVCF', expr('CASE WHEN WBC_LVCF = 3 THEN 1 ELSE 0 END'))
    # df = df.withColumn('wbc_covariate_low_LVCF', expr('CASE WHEN WBC_LVCF = 2 THEN 1 ELSE 0 END'))
    # df = df.withColumn('wbc_covariate_normal_LVCF', expr('CASE WHEN WBC_LVCF = 1 THEN 1 ELSE 0 END'))
    # df = df.withColumn('lymph_covariate_high_LVCF', expr('CASE WHEN LYMPH_LVCF = 3 THEN 1 ELSE 0 END'))
    # df = df.withColumn('lymph_covariate_low_LVCF', expr('CASE WHEN LYMPH_LVCF = 2 THEN 1 ELSE 0 END'))
    # df = df.withColumn('lymph_covariate_normal_LVCF', expr('CASE WHEN LYMPH_LVCF = 1 THEN 1 ELSE 0 END'))
    # df = df.withColumn('albumin_covariate_high_LVCF', expr('CASE WHEN ALBUMIN_LVCF = 3 THEN 1 ELSE 0 END'))
    # df = df.withColumn('albumin_covariate_low_LVCF', expr('CASE WHEN ALBUMIN_LVCF = 2 THEN 1 ELSE 0 END'))
    # df = df.withColumn('albumin_covariate_normal_LVCF', expr('CASE WHEN ALBUMIN_LVCF = 1 THEN 1 ELSE 0 END'))
    df = df.withColumn('alt_covariate_high_LVCF', expr('CASE WHEN ALT_LVCF = 3 THEN 1 ELSE 0 END'))
    df = df.withColumn('alt_covariate_low_LVCF', expr('CASE WHEN ALT_LVCF = 2 THEN 1 ELSE 0 END'))
    df = df.withColumn('alt_covariate_normal_LVCF', expr('CASE WHEN ALT_LVCF = 1 THEN 1 ELSE 0 END'))
    df = df.withColumn('egfr_covariate_low_LVCF', expr('CASE WHEN EGFR_LVCF = 2 THEN 1 ELSE 0 END'))
    df = df.withColumn('egfr_covariate_normal_LVCF', expr('CASE WHEN EGFR_LVCF = 1 THEN 1 ELSE 0 END'))
    # df = df.withColumn('plt_covariate_high_LVCF', expr('CASE WHEN PLT_LVCF = 3 THEN 1 ELSE 0 END'))
    # df = df.withColumn('plt_covariate_low_LVCF', expr('CASE WHEN PLT_LVCF = 2 THEN 1 ELSE 0 END'))
    # df = df.withColumn('plt_covariate_normal', expr('CASE WHEN PLT_LVCF = 1 THEN 1 ELSE 0 END'))
    df = df.withColumn('creatinine_covariate_high', expr('CASE WHEN CREATININE_LVCF = 3 THEN 1 ELSE 0 END'))
    df = df.withColumn('creatinine_covariate_low', expr('CASE WHEN CREATININE_LVCF = 2 THEN 1 ELSE 0 END'))
    df = df.withColumn('creatinine_covariate_normal_LVCF', expr('CASE WHEN CREATININE_LVCF = 1 THEN 1 ELSE 0 END'))
    # df = df.withColumn('hemoglobin_covariate_high_LVCF', expr('CASE WHEN HEMOGLOBIN_LVCF = 3 THEN 1 ELSE 0 END'))
    # df = df.withColumn('hemoglobin_covariate_low_LVCF', expr('CASE WHEN HEMOGLOBIN_LVCF = 2 THEN 1 ELSE 0 END'))
    # df = df.withColumn('hemoglobin_covariate_normal_LVCF', expr('CASE WHEN HEMOGLOBIN_LVCF = 1 THEN 1 ELSE 0 END'))
    # df = df.withColumn('diabetes_a1c_covariate_high_LVCF', expr('CASE WHEN DIABETES_A1C_LVCF = 2 THEN 1 ELSE 0 END'))
    # df = df.withColumn('diabetes_a1c_covariate_normal_LVCF', expr('CASE WHEN DIABETES_A1C_LVCF = 1 THEN 1 ELSE 0 END'))

    # # Finally, perform categorizations of the most recent A1c for comparison
    # df = df.withColumn('current_A1c_categorical', expr("CASE \
    # WHEN DIABETES_A1C_current_LVCF < 4 THEN 'Less400' \
    # WHEN DIABETES_A1C_current_LVCF < 5.3 THEN '400to529' \
    # WHEN DIABETES_A1C_current_LVCF < 5.6 THEN '530to559' \
    # WHEN DIABETES_A1C_current_LVCF < 5.9 THEN '560to589' \
    # WHEN DIABETES_A1C_current_LVCF < 6.2 THEN '590to619' \
    # WHEN DIABETES_A1C_current_LVCF < 6.5 THEN '620to649' \
    # WHEN DIABETES_A1C_current_LVCF >= 6.5 THEN '650plus' \
    # ELSE 'MissingA1c' END"))

    # # Finally, perform categorizations of the most recent A1c for comparison
    # df = df.withColumn('current_A1c_categorical', expr("CASE \
    # WHEN DIABETES_A1C_LVCF_CONTINUOUS < 5.3 THEN '400to529' \
    # WHEN DIABETES_A1C_LVCF_CONTINUOUS < 5.6 THEN '530to559' \
    # WHEN DIABETES_A1C_LVCF_CONTINUOUS < 5.9 THEN '560to589' \
    # WHEN DIABETES_A1C_LVCF_CONTINUOUS < 6.2 THEN '590to619' \
    # WHEN DIABETES_A1C_LVCF_CONTINUOUS >= 6.2 THEN '620to649' \
    # ELSE 'MissingA1c' END")).drop('DIABETES_A1C_LVCF_CONTINUOUS')

    # # Convert the above to dummies, and drop the original features
    # gps = df.select('current_A1c_categorical').distinct().toPandas()
    # gps = gps['current_A1c_categorical'].tolist()
    # for i in gps:
    #     df = df.withColumn('current_A1c_covariate_{}_LVCF'.format(i), expr('CASE WHEN current_A1c_categorical = "{}" THEN 1 ELSE 0 END'.format(i)))
    # df = df.drop('current_A1c_categorical')

    # #############################################################

    return df

    

    
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.2829da0b-d0cc-4a0e-8ecb-d8118849f408"),
    cohort_code=Input(rid="ri.foundry.main.dataset.b6450c6b-55d3-41e3-8208-14fa0780cd41"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    drugs_filtered=Input(rid="ri.foundry.main.dataset.279e6680-9619-4a95-924d-90510ef7a9ab"),
    measurement=Input(rid="ri.foundry.main.dataset.d6054221-ee0c-4858-97de-22292458fa19")
)
#### THIS HAS MULTIPLE ROWS PER PATIENT

def measurements_filtered(measurement, concept_set_members, cohort_code, drugs_filtered):
    
    # #bring in only cohort patient ids
    # persons = cohort_code.select('person_id')
    
    # # Filter measurements table to our cohort. This table is patient/date - we keep only rows that are non null. 
    # df = measurement.select('person_id','measurement_date','measurement_concept_id','harmonized_value_as_number', 'value_as_concept_id').where(col('measurement_date').isNotNull()).withColumnRenamed('measurement_date','date').join(persons,'person_id','inner')
    
    # # Filter the concept set table to most recent version
    # concepts_df = concept_set_members.select('concept_set_name', 'is_most_recent_version', 'concept_id').where(col('is_most_recent_version')=='true')
          
    # # Create objects specifying lowest and highest acceptable BMI, height and weight
    # lowest_acceptable_BMI = 10
    # highest_acceptable_BMI = 100
    # lowest_acceptable_weight = 5 #in kgs
    # highest_acceptable_weight = 300 #in kgs
    # lowest_acceptable_height = 0.6 #in meters
    # highest_acceptable_height = 2.43 #in meters
    
    # # Filter the concepts sets table to various measurement concepts like BMI, Height and Weight. Save these as lists 
    # bmi_codeset_ids = list(concepts_df.where(
    #     (concepts_df.concept_set_name=="body mass index") 
    #     & (concepts_df.is_most_recent_version=='true')
    #     ).select('concept_id').toPandas()['concept_id'])
        
    # weight_codeset_ids = list(concepts_df.where(
    #     (concepts_df.concept_set_name=="Body weight (LG34372-9 and SNOMED)") 
    #     & (concepts_df.is_most_recent_version=='true')
    #     ).select('concept_id').toPandas()['concept_id'])
        
    # height_codeset_ids = list(concepts_df.where(
    #     (concepts_df.concept_set_name=="Height (LG34373-7 + SNOMED)") 
    #     & (concepts_df.is_most_recent_version=='true')
    #     ).select('concept_id').toPandas()['concept_id'])
    
    # # Filter the concept sets table to COVID related measurement concept terms: For both COVID-related tests and results. Save these as lists
    # pcr_ag_test_ids = list(concepts_df.where(
    #     (concepts_df.concept_set_name=="ATLAS SARS-CoV-2 rt-PCR and AG") 
    #     & (concepts_df.is_most_recent_version=='true')
    #     ).select('concept_id').toPandas()['concept_id'])
        
    # antibody_test_ids = list(concepts_df.where(
    #     (concepts_df.concept_set_name=="Atlas #818 [N3C] CovidAntibody retry") 
    #     & (concepts_df.is_most_recent_version=='true')
    #     ).select('concept_id').toPandas()['concept_id'])
        
    # covid_positive_measurement_ids = list(concepts_df.where(
    #     (concepts_df.concept_set_name=="ResultPos") 
    #     & (concepts_df.is_most_recent_version=='true')
    #     ).select('concept_id').toPandas()['concept_id'])
        
    # covid_negative_measurement_ids = list(concepts_df.where(
    #     (concepts_df.concept_set_name=="ResultNeg") 
    #     & (concepts_df.is_most_recent_version=='true')
    #     ).select('concept_id').toPandas()['concept_id'])

    # # Use the measurements table - first filter to nonnull rows. For rows where the measurement_concept_id is in the list of BMI concept sets AND is between the acceptable limits, use the "harmonized_value_as_number" value for the new BMI column "Recorded_BMI". Do the same for height and weight
    # BMI_df = df.where(col('harmonized_value_as_number').isNotNull()).withColumn('Recorded_BMI', when(col('measurement_concept_id').isin(bmi_codeset_ids) & col('harmonized_value_as_number').between(lowest_acceptable_BMI, highest_acceptable_BMI), col('harmonized_value_as_number')).otherwise(0)).withColumn('height', when(col('measurement_concept_id').isin(height_codeset_ids) & col('harmonized_value_as_number').between(lowest_acceptable_height, highest_acceptable_height), col('harmonized_value_as_number')).otherwise(0)).withColumn('weight', when(col('measurement_concept_id').isin(weight_codeset_ids) & col('harmonized_value_as_number').between(lowest_acceptable_weight, highest_acceptable_weight), col('harmonized_value_as_number')).otherwise(0)) 
    
    # # When the PCR COVID test is in the list of COVID test concepts, AND the result is positive >> make the cell 1 (Otherwise 0). Repeat for Antigen Tests
    # labs_df = df.withColumn('PCR_AG_Pos', when(col('measurement_concept_id').isin(pcr_ag_test_ids) & col('value_as_concept_id').isin(covid_positive_measurement_ids), 1).otherwise(0)).withColumn('PCR_AG_Neg', when(col('measurement_concept_id').isin(pcr_ag_test_ids) & col('value_as_concept_id').isin(covid_negative_measurement_ids), 1).otherwise(0)).withColumn('Antibody_Pos', when(col('measurement_concept_id').isin(antibody_test_ids) & col('value_as_concept_id').isin(covid_positive_measurement_ids), 1).otherwise(0)).withColumn('Antibody_Neg', when(col('measurement_concept_id').isin(antibody_test_ids) & col('value_as_concept_id').isin(covid_negative_measurement_ids), 1).otherwise(0))
     
    # # For the BMI table - groupby person_id and date and take the maximum value for BMI, height and weight. Repeat for the test type / result type combinations
    # # At this point we will only have rows for patients/dates where at least one of the measures was recorded. 
    # BMI_df = BMI_df.groupby('person_id', 'date').agg(
    # max('Recorded_BMI').alias('Recorded_BMI'),
    # max('height').alias('height'),
    # max('weight').alias('weight'))
    
    # labs_df = labs_df.groupby('person_id', 'date').agg(
    # max('PCR_AG_Pos').alias('PCR_AG_Pos'),
    # max('PCR_AG_Neg').alias('PCR_AG_Neg'),
    # max('Antibody_Pos').alias('Antibody_Pos'),
    # max('Antibody_Neg').alias('Antibody_Neg'))

    # # Add some calculated columns for calculated BMI based on height and weight 
    # BMI_df = BMI_df.withColumn('calculated_BMI', (col('weight')/(col('height')*col('height'))))
    
    # # If the recorded BMI is > 0 use that value. Otherwise use the calculated BMI column. COnsolidate into a single "BMI" column
    # BMI_df = BMI_df.withColumn('BMI', when(col('Recorded_BMI')>0, col('Recorded_BMI')).otherwise(col('calculated_BMI'))).select('person_id','date','BMI')
    
    # # Keep only rows where the BMI falls in between the acceptable limits (the objects created above). Round the BMI column and save in a new column "BMI_rounded"
    # BMI_df = BMI_df.where((col('BMI')<=highest_acceptable_BMI) & (col('BMI')>=lowest_acceptable_BMI)).withColumn('BMI_rounded', round(col('BMI'))).drop('BMI')
    
    # # Create an OBESITY indicator column 
    # BMI_df = BMI_df.withColumn('OBESITY', when(col('BMI_rounded') >=30, 1).otherwise(0))

    # # Left join >> only join BMI rows where lab measurements (COVID TESTS and other concepts) occurred
    # df = labs_df.join(BMI_df, on=['person_id', 'date'], how='left')

    # return df
    #########################################

    #bring in only cohort patient ids
    persons = cohort_code.select('person_id')

    # Filter the concept set table to most recent version AND concepts we use here
    concepts_df = concept_set_members.select('concept_set_name', 'is_most_recent_version', 'concept_id').where(expr("is_most_recent_version = 'true' AND concept_set_name IN ('body mass index','Body weight (LG34372-9 and SNOMED)','Height (LG34373-7 + SNOMED)','ATLAS SARS-CoV-2 rt-PCR and AG','Atlas #818 [N3C] CovidAntibody retry','ResultPos','ResultNeg')")).withColumnRenamed('concept_id', 'measurement_concept_id').distinct()
    

    # The measurement table is patient/date - we keep only rows that are non null. We then filter measurement table first to the concepts sets above, then to our cohort. 
    df = measurement.select('person_id','measurement_date','measurement_concept_id','harmonized_value_as_number', 'value_as_concept_id').where(col('measurement_date').isNotNull()).withColumnRenamed('measurement_date','date').join(concepts_df.select('measurement_concept_id'), on = 'measurement_concept_id', how = 'inner').join(persons, on = 'person_id', how = 'inner')

    # Rename the measurement_concept_id back to concept_id
    concepts_df = concepts_df.withColumnRenamed('measurement_concept_id', 'concept_id')
          
    # Create objects specifying lowest and highest acceptable BMI, height and weight
    lowest_acceptable_BMI = 10
    highest_acceptable_BMI = 100
    lowest_acceptable_weight = 5 #in kgs
    highest_acceptable_weight = 300 #in kgs
    lowest_acceptable_height = 0.6 #in meters
    highest_acceptable_height = 2.43 #in meters
    
    # Filter the concepts sets table to various measurement concepts like BMI, Height and Weight. Save these as lists 
    bmi_codeset_ids = list(concepts_df.where(
        (concepts_df.concept_set_name=="body mass index") 
        & (concepts_df.is_most_recent_version=='true')
        ).select('concept_id').toPandas()['concept_id'])
        
    weight_codeset_ids = list(concepts_df.where(
        (concepts_df.concept_set_name=="Body weight (LG34372-9 and SNOMED)") 
        & (concepts_df.is_most_recent_version=='true')
        ).select('concept_id').toPandas()['concept_id'])
        
    height_codeset_ids = list(concepts_df.where(
        (concepts_df.concept_set_name=="Height (LG34373-7 + SNOMED)") 
        & (concepts_df.is_most_recent_version=='true')
        ).select('concept_id').toPandas()['concept_id'])
    
    # Filter the concept sets table to COVID related measurement concept terms: For both COVID-related tests and results. Save these as lists
    pcr_ag_test_ids = list(concepts_df.where(
        (concepts_df.concept_set_name=="ATLAS SARS-CoV-2 rt-PCR and AG") 
        & (concepts_df.is_most_recent_version=='true')
        ).select('concept_id').toPandas()['concept_id'])
        
    antibody_test_ids = list(concepts_df.where(
        (concepts_df.concept_set_name=="Atlas #818 [N3C] CovidAntibody retry") 
        & (concepts_df.is_most_recent_version=='true')
        ).select('concept_id').toPandas()['concept_id'])
        
    covid_positive_measurement_ids = list(concepts_df.where(
        (concepts_df.concept_set_name=="ResultPos") 
        & (concepts_df.is_most_recent_version=='true')
        ).select('concept_id').toPandas()['concept_id'])
        
    covid_negative_measurement_ids = list(concepts_df.where(
        (concepts_df.concept_set_name=="ResultNeg") 
        & (concepts_df.is_most_recent_version=='true')
        ).select('concept_id').toPandas()['concept_id'])

    # Use the measurements table - first filter to nonnull rows. For rows where the measurement_concept_id is in the list of BMI concept sets AND is between the acceptable limits, use the "harmonized_value_as_number" value for the new BMI column "Recorded_BMI". Do the same for height and weight
    BMI_df = df.where(col('harmonized_value_as_number').isNotNull()).withColumn('Recorded_BMI', when(col('measurement_concept_id').isin(bmi_codeset_ids) & col('harmonized_value_as_number').between(lowest_acceptable_BMI, highest_acceptable_BMI), col('harmonized_value_as_number')).otherwise(0)).withColumn('height', when(col('measurement_concept_id').isin(height_codeset_ids) & col('harmonized_value_as_number').between(lowest_acceptable_height, highest_acceptable_height), col('harmonized_value_as_number')).otherwise(0)).withColumn('weight', when(col('measurement_concept_id').isin(weight_codeset_ids) & col('harmonized_value_as_number').between(lowest_acceptable_weight, highest_acceptable_weight), col('harmonized_value_as_number')).otherwise(0)) 
    
    # When the PCR COVID test is in the list of COVID test concepts, AND the result is positive >> make the cell 1 (Otherwise 0). Repeat for Antigen Tests
    labs_df = df.withColumn('PCR_AG_Pos', when(col('measurement_concept_id').isin(pcr_ag_test_ids) & col('value_as_concept_id').isin(covid_positive_measurement_ids), 1).otherwise(0)).withColumn('PCR_AG_Neg', when(col('measurement_concept_id').isin(pcr_ag_test_ids) & col('value_as_concept_id').isin(covid_negative_measurement_ids), 1).otherwise(0)).withColumn('Antibody_Pos', when(col('measurement_concept_id').isin(antibody_test_ids) & col('value_as_concept_id').isin(covid_positive_measurement_ids), 1).otherwise(0)).withColumn('Antibody_Neg', when(col('measurement_concept_id').isin(antibody_test_ids) & col('value_as_concept_id').isin(covid_negative_measurement_ids), 1).otherwise(0))
     
    # For the BMI table - groupby person_id and date and take the maximum value for BMI, height and weight. Repeat for the test type / result type combinations
    # At this point we will only have rows for patients/dates where at least one of the measures was recorded. 
    BMI_df = BMI_df.groupby('person_id', 'date').agg(
    max('Recorded_BMI').alias('Recorded_BMI'),
    max('height').alias('height'),
    max('weight').alias('weight'))
    
    labs_df = labs_df.groupby('person_id', 'date').agg(
    max('PCR_AG_Pos').alias('PCR_AG_Pos'),
    max('PCR_AG_Neg').alias('PCR_AG_Neg'),
    max('Antibody_Pos').alias('Antibody_Pos'),
    max('Antibody_Neg').alias('Antibody_Neg'))

    # Add some calculated columns for calculated BMI based on height and weight 
    BMI_df = BMI_df.withColumn('calculated_BMI', (col('weight')/(col('height')*col('height'))))
    
    # If the recorded BMI is > 0 use that value. Otherwise use the calculated BMI column. COnsolidate into a single "BMI" column
    BMI_df = BMI_df.withColumn('BMI', when(col('Recorded_BMI')>0, col('Recorded_BMI')).otherwise(col('calculated_BMI'))).select('person_id','date','BMI')
    
    # Keep only rows where the BMI falls in between the acceptable limits (the objects created above). Round the BMI column and save in a new column "BMI_rounded"
    BMI_df = BMI_df.where((col('BMI')<=highest_acceptable_BMI) & (col('BMI')>=lowest_acceptable_BMI)).withColumn('BMI_rounded', round(col('BMI'))).drop('BMI')
    
    # Create an OBESITY indicator column 
    BMI_df = BMI_df.withColumn('OBESITY', when(col('BMI_rounded') >=30, 1).otherwise(0))

    # Left join >> only join BMI rows where lab measurements (COVID TESTS and other concepts) occurred
    df = labs_df.join(BMI_df, on=['person_id', 'date'], how='left')

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b5e5c84a-33bd-46d9-8620-5d95eca65f9c"),
    paxlovid_marginal_survival_curve_LC=Input(rid="ri.foundry.main.dataset.a0cf0300-89fe-4b0b-8ce5-2a3701f600d3"),
    paxlovid_marginal_survival_curve_composite=Input(rid="ri.foundry.main.dataset.15d987cb-52c0-4fae-a754-b2cc581c4a05"),
    paxlovid_marginal_survival_curve_compositedeathhosp=Input(rid="ri.foundry.main.dataset.c491c8ca-8112-422e-b361-d46b411e524d"),
    paxlovid_marginal_survival_curve_death=Input(rid="ri.foundry.main.dataset.5510c5c0-44e4-4f2a-a943-17e7769544d6"),
    paxlovid_marginal_survival_curve_modcomposite=Input(rid="ri.foundry.main.dataset.eea6696b-8640-4f79-86e9-d53c9aa772a7")
)
def merge_results(paxlovid_marginal_survival_curve_composite, paxlovid_marginal_survival_curve_modcomposite, paxlovid_marginal_survival_curve_compositedeathhosp, paxlovid_marginal_survival_curve_death, paxlovid_marginal_survival_curve_LC):

    # ("Relative_Risk","Relative_Risk_Lower_Limit","Relative_Risk_Upper_Limit","Risk_Reduction","Risk_Reduction_Lower_Limit","Risk_Reduction_Upper_Limit","drug")
    df1A = paxlovid_marginal_survival_curve_composite.loc[:, ['Relative_Risk','Relative_Risk_Lower_Limit','Relative_Risk_Upper_Limit']]
    df1A['Outcome'] = 'Composite - ED visit, hospitalization, supplemental oxygen, or death'
    df1A['Statistic'] = 'Relative Risk'
    df1A = df1A.rename(columns = {'Relative_Risk_Lower_Limit':'Lower_Limit', 'Relative_Risk_Upper_Limit':'Upper_Limit','Relative_Risk':'Estimate'})
    df1A['rank'] = 1

    df1B = paxlovid_marginal_survival_curve_composite.loc[:, ['Risk_Reduction','Risk_Reduction_Lower_Limit','Risk_Reduction_Upper_Limit']]
    df1B['Outcome'] = 'Composite - ED visit, hospitalization, supplemental oxygen, or death'
    df1B['Statistic'] = 'Risk Reduction'
    df1B = df1B.rename(columns = {'Risk_Reduction_Lower_Limit':'Lower_Limit', 'Risk_Reduction_Upper_Limit':'Upper_Limit','Risk_Reduction':'Estimate'})
    df1B['rank'] = 5

    df1 = pd.concat([df1A, df1B])

    df2A = paxlovid_marginal_survival_curve_modcomposite.loc[:, ['Relative_Risk','Relative_Risk_Lower_Limit','Relative_Risk_Upper_Limit']]
    df2A['Outcome'] = 'Composite - ED visit, hospitalization, or death'
    df2A['Statistic'] = 'Relative Risk'
    df2A = df2A.rename(columns = {'Relative_Risk_Lower_Limit':'Lower_Limit', 'Relative_Risk_Upper_Limit':'Upper_Limit','Relative_Risk':'Estimate'})
    df2A['rank'] = 2

    df2B = paxlovid_marginal_survival_curve_modcomposite.loc[:, ['Risk_Reduction','Risk_Reduction_Lower_Limit','Risk_Reduction_Upper_Limit']]
    df2B['Outcome'] = 'Composite - ED visit, hospitalization, or death'
    df2B['Statistic'] = 'Risk Reduction'
    df2B = df2B.rename(columns = {'Risk_Reduction_Lower_Limit':'Lower_Limit', 'Risk_Reduction_Upper_Limit':'Upper_Limit','Risk_Reduction':'Estimate'})
    df2B['rank'] = 6

    df2 = pd.concat([df2A, df2B])

    df3A = paxlovid_marginal_survival_curve_compositedeathhosp.loc[:, ['Relative_Risk','Relative_Risk_Lower_Limit','Relative_Risk_Upper_Limit']]
    df3A['Outcome'] = 'Composite - hospitalization or death'
    df3A['Statistic'] = 'Relative Risk'
    df3A = df3A.rename(columns = {'Relative_Risk_Lower_Limit':'Lower_Limit', 'Relative_Risk_Upper_Limit':'Upper_Limit','Relative_Risk':'Estimate'})
    df3A['rank'] = 3

    df3B = paxlovid_marginal_survival_curve_compositedeathhosp.loc[:, ['Risk_Reduction','Risk_Reduction_Lower_Limit','Risk_Reduction_Upper_Limit']]
    df3B['Outcome'] = 'Composite - hospitalization or death'
    df3B['Statistic'] = 'Risk Reduction'
    df3B = df3B.rename(columns = {'Risk_Reduction_Lower_Limit':'Lower_Limit', 'Risk_Reduction_Upper_Limit':'Upper_Limit','Risk_Reduction':'Estimate'})
    df3B['rank'] = 7
    df3 = pd.concat([df3A, df3B])

    df4A = paxlovid_marginal_survival_curve_death.loc[:, ['Relative_Risk','Relative_Risk_Lower_Limit','Relative_Risk_Upper_Limit']]
    df4A['Outcome'] = 'Death'
    df4A['Statistic'] = 'Relative Risk'
    df4A = df4A.rename(columns = {'Relative_Risk_Lower_Limit':'Lower_Limit', 'Relative_Risk_Upper_Limit':'Upper_Limit','Relative_Risk':'Estimate'})
    df4A['rank'] = 4

    df4B = paxlovid_marginal_survival_curve_death.loc[:, ['Risk_Reduction','Risk_Reduction_Lower_Limit','Risk_Reduction_Upper_Limit']]
    df4B['Outcome'] = 'Death'
    df4B['Statistic'] = 'Risk Reduction'
    df4B = df4B.rename(columns = {'Risk_Reduction_Lower_Limit':'Lower_Limit', 'Risk_Reduction_Upper_Limit':'Upper_Limit','Risk_Reduction':'Estimate'})
    df4B['rank'] = 8
    df4 = pd.concat([df4A, df4B])

    df5A = paxlovid_marginal_survival_curve_LC.loc[:, ['Relative_Risk','Relative_Risk_Lower_Limit','Relative_Risk_Upper_Limit']]
    df5A['Outcome'] = 'Long COVID'
    df5A['Statistic'] = 'Relative Risk'
    df5A = df5A.rename(columns = {'Relative_Risk_Lower_Limit':'Lower_Limit', 'Relative_Risk_Upper_Limit':'Upper_Limit','Relative_Risk':'Estimate'})
    df5A['rank'] = 5

    df5B = paxlovid_marginal_survival_curve_LC.loc[:, ['Risk_Reduction','Risk_Reduction_Lower_Limit','Risk_Reduction_Upper_Limit']]
    df5B['Outcome'] = 'Long COVID'
    df5B['Statistic'] = 'Risk Reduction'
    df5B = df5B.rename(columns = {'Risk_Reduction_Lower_Limit':'Lower_Limit', 'Risk_Reduction_Upper_Limit':'Upper_Limit','Risk_Reduction':'Estimate'})
    df5B['rank'] = 10
    df5 = pd.concat([df5A, df5B])

    final = pd.concat([df1,df2,df3,df4,df5])

    return final
     
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3ce973a0-3ec4-41f6-acc5-7b71551d38c4"),
    Paxlovid_Long_COVID_DTSA=Input(rid="ri.foundry.main.dataset.ca282b12-df63-4d80-a1d6-245d446596a0"),
    coxreg_matching_composite=Input(rid="ri.foundry.main.dataset.02d290e4-679c-4bc4-8950-2c8582620696"),
    coxreg_matching_composite_death_hosp=Input(rid="ri.foundry.main.dataset.f09ea15f-655a-4eb2-828c-9a3520200d66"),
    coxreg_matching_death=Input(rid="ri.foundry.main.dataset.7a61c0b5-0bfa-4455-8e47-28de0483238b"),
    coxreg_matching_modcomposite=Input(rid="ri.foundry.main.dataset.51246f0a-8d87-4c6e-b97f-9bdbccc389df")
)
def merge_results_HRs(coxreg_matching_composite, coxreg_matching_modcomposite, coxreg_matching_composite_death_hosp, coxreg_matching_death, Paxlovid_Long_COVID_DTSA):

    # ("coef","HR","SE","Robust_SE","z","P","variable","adjusted_LL","adjusted_UL","adjusted_adjusted_LL","adjusted_adjusted_UL")
    df1 = coxreg_matching_composite.loc[:, ['HR','adjusted_LL','adjusted_UL']]
    df1['Outcome'] = 'Composite - ED visit, hospitalization, supplemental oxygen, or death'
    df1['Statistic'] = 'Hazard Ratio'
    df1 = df1.rename(columns = {'adjusted_LL':'Lower_Limit', 'adjusted_UL':'Upper_Limit','HR':'Estimate'})
    df1['rank'] = 1

    df2 = coxreg_matching_modcomposite.loc[:, ['HR','adjusted_LL','adjusted_UL']]
    df2['Outcome'] = 'Composite - ED visit, hospitalization, or death'
    df2['Statistic'] = 'Hazard Ratio'
    df2 = df2.rename(columns = {'adjusted_LL':'Lower_Limit', 'adjusted_UL':'Upper_Limit','HR':'Estimate'})
    df2['rank'] = 2

    df3 = coxreg_matching_composite_death_hosp.loc[:, ['HR','adjusted_LL','adjusted_UL']]
    df3['Outcome'] = 'Composite - hospitalization or death'
    df3['Statistic'] = 'Hazard Ratio'
    df3 = df3.rename(columns = {'adjusted_LL':'Lower_Limit', 'adjusted_UL':'Upper_Limit','HR':'Estimate'})
    df3['rank'] = 3

    df4 = coxreg_matching_death.loc[:, ['HR','adjusted_LL','adjusted_UL']]
    df4['Outcome'] = 'Death'
    df4['Statistic'] = 'Hazard Ratio'
    df4 = df4.rename(columns = {'adjusted_LL':'Lower_Limit', 'adjusted_UL':'Upper_Limit','HR':'Estimate'})
    df4['rank'] = 4

    # ("Estimate","SE","t","P","variable","RR","LL","UL","adjusted_LL","adjusted_UL")

    df5 = Paxlovid_Long_COVID_DTSA.loc[:, ['RR','adjusted_LL','adjusted_UL']]
    df5['Outcome'] = 'Long COVID'
    df5['Statistic'] = 'Hazard Ratio'
    df5 = df5.rename(columns = {'adjusted_LL':'Lower_Limit', 'adjusted_UL':'Upper_Limit','RR':'Estimate'})
    df5['rank'] = 5

    final = pd.concat([df1,df2,df3,df4,df5])

    return final
     
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.9b0b693d-1e29-4eeb-aa02-a86f1c7c7fb6"),
    cohort_code=Input(rid="ri.foundry.main.dataset.b6450c6b-55d3-41e3-8208-14fa0780cd41"),
    conditions_filtered=Input(rid="ri.foundry.main.dataset.3af1cb46-7f36-4210-8519-70b0a8d54302"),
    drugs_filtered=Input(rid="ri.foundry.main.dataset.279e6680-9619-4a95-924d-90510ef7a9ab"),
    filtered_lab_measures_harmonized_cleaned_final=Input(rid="ri.foundry.main.dataset.90b57f36-aaeb-4f6d-8831-6141389595e0"),
    measurements_filtered=Input(rid="ri.foundry.main.dataset.2829da0b-d0cc-4a0e-8ecb-d8118849f408"),
    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.5af2c604-51e0-4afa-b1ae-1e5fa2f4b905")
)
def minimal_patient_cohort(drugs_filtered, cohort_code, microvisits_to_macrovisits, filtered_lab_measures_harmonized_cleaned_final, conditions_filtered, measurements_filtered):

    # This node will identify which patients are potentially eligible for the study
    # At a minimum: 30+, Type 2 diabetes, known BMI, 12 months of data (pre COVID observation period), Known HbA1c (lab data)

    minimum_age = 18
    trial_start_date = '2021-12-01'
    drug_columns = ['PAXLOVID']
    grace_period = 5
    keep_patients_missing_bmi = True

    df = cohort_code.select('person_id','COVID_first_poslab_or_diagnosis_date','sex','date_of_birth','age_at_covid','observation_period_before_covid','race_ethnicity','data_partner_id','data_extraction_date')

    # 1. Patient has age and sex data
    df = df.where(expr('date_of_birth IS NOT NULL')).where(expr("sex in ('MALE','FEMALE')"))
    print('Patient has sex and age data', print(df.select(countDistinct(col('person_id'))).toPandas()))

    # Patient had COVID-19 on or after the start date
    df = df.where(expr("COVID_first_poslab_or_diagnosis_date >= '{}'".format(trial_start_date)))
    print('Patient has COVID after the EUA date', print(df.select(countDistinct(col('person_id'))).toPandas()))

    # 6. Patient has BMI measurement (before COVID up to 7 days after)
    bmi_df = measurements_filtered.select('person_id','date','BMI_rounded').where(expr('BMI_rounded IS NOT NULL')).withColumnRenamed('date','bmi_date')
    bmi_df = df.select('person_id','COVID_first_poslab_or_diagnosis_date').withColumn('modified_index_date', expr('DATE_ADD(COVID_first_poslab_or_diagnosis_date, {grace} - 1)'.format(grace = grace_period))).join(bmi_df, on = 'person_id', how = 'inner')
    bmi_df = bmi_df.where(expr('bmi_date <= modified_index_date')).withColumn('recent_bmi_date', expr('MAX(bmi_date) OVER(PARTITION BY person_id)')).select('person_id','recent_bmi_date').distinct()

    # Join back to the main data frame
    if keep_patients_missing_bmi:
        df = df.join(bmi_df, on = 'person_id', how = 'left')
    else:
        df = df.join(bmi_df, on = 'person_id', how = 'inner')
    print('Patient has a BMI measurement (up to day 5 since COVID)', print(df.select(countDistinct(col('person_id'))).toPandas()))

    return df

    

    
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6151c319-494e-4b79-b05e-9533024a6174"),
    cohort_code=Input(rid="ri.foundry.main.dataset.b6450c6b-55d3-41e3-8208-14fa0780cd41"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    conditions_filtered=Input(rid="ri.foundry.main.dataset.3af1cb46-7f36-4210-8519-70b0a8d54302"),
    customize_concept_sets=Input(rid="ri.foundry.main.dataset.de3b19a8-bb5d-4752-a040-4e338b103af9"),
    observation=Input(rid="ri.foundry.main.dataset.b998b475-b229-471c-800e-9421491409f3")
)
def observations_filtered(observation, concept_set_members, cohort_code, customize_concept_sets, conditions_filtered):

    # Create a table of person_ids for our cohort
    persons = cohort_code.select('person_id')
    
    # Inner join the observations table to our cohort
    observations_df = observation.select('person_id','observation_date','observation_concept_id').where(col('observation_date').isNotNull()).withColumnRenamed('observation_date','date').withColumnRenamed('observation_concept_id','concept_id').join(persons,'person_id','inner')

    # Filter the fusion sheet to "observations"
    fusion_df = customize_concept_sets.where(col('domain').contains('observation')).select('concept_set_name','indicator_prefix')
    
    # Filter the concept set table to just our observation concept sets of interest by joining to our fusion sheet
    concepts_df = concept_set_members.select('concept_set_name', 'is_most_recent_version', 'concept_id').where(col('is_most_recent_version')=='true').join(fusion_df, 'concept_set_name', 'inner').select('concept_id','indicator_prefix')

    # Filter the observations_df table above to only observations in our concept sets of interest
    # At this point we have multiple rows per patient/observation [indicator_prefix]/date
    df = observations_df.join(concepts_df, 'concept_id', 'inner')
    
    # CREATE PIVOT TABLE: Group by will put patient and date along the rows, and pivoting by indicator_prefix places each unique observation type along the columns. The aggregtion will simply be to place a 1 in that cell (if the observation exists for the patient/date)    
    df = df.groupby('person_id','date').pivot('indicator_prefix').agg(lit(1)).na.fill(0)

    return df

#################################################
## Global imports and functions included below ##
#################################################

from pyspark.sql import functions as F

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.91a6e009-64ca-466c-b3ba-34b2a086b9f8"),
    ED_visits=Input(rid="ri.foundry.main.dataset.0ef602f8-450d-43f1-a28f-b0dc7947143a"),
    cohort_facts_visit_level_paxlovid=Input(rid="ri.foundry.main.dataset.63c6f325-61d9-4e4f-acc2-2e9e6c301adc")
)
def oxygen_date(cohort_facts_visit_level_paxlovid, ED_visits):
    cohort_facts_visit_level_covid_out = cohort_facts_visit_level_paxlovid

    oxygen = cohort_facts_visit_level_covid_out.select('person_id','date','supplemental_oxygen_procedure','COVID_first_poslab_or_diagnosis_date').where(expr('supplemental_oxygen_procedure = 1'))

    result = oxygen.withColumn('pre_post_covid', expr('CASE WHEN date < COVID_first_poslab_or_diagnosis_date THEN 1 WHEN date >= COVID_first_poslab_or_diagnosis_date THEN 2 ELSE NULL END'))
    result = result.withColumn('oxygen_date_post_covid', expr('MIN(CASE WHEN pre_post_covid = 2 THEN date ELSE NULL END) OVER(PARTITION BY person_id)'))
    result = result.withColumn('oxygen_date_30_days_pre_covid', expr('MAX(CASE WHEN date BETWEEN DATE_ADD(COVID_first_poslab_or_diagnosis_date, -31) AND DATE_ADD(COVID_first_poslab_or_diagnosis_date, -1) THEN date ELSE NULL END) OVER(PARTITION BY person_id)'))
    result = result.withColumn('earliest_oxygen_date', expr('MIN(date) OVER(PARTITION BY person_id)'))
    result = result.withColumn('oxygen_date_pre_covid', expr('MAX(CASE WHEN pre_post_covid = 1 THEN date ELSE NULL END) OVER(PARTITION BY person_id)'))

    return result.select('person_id','oxygen_date_post_covid','oxygen_date_pre_covid').distinct().where(expr('NOT(oxygen_date_post_covid IS NULL AND oxygen_date_pre_covid IS NULL)'))
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b8ad3326-707a-4073-9e8f-0c8317bdfb2e"),
    expanded_dataset_for_outcome_analysis_matching=Input(rid="ri.foundry.main.dataset.32ef28d9-6ad8-4b41-8a63-c6e4e37036d9"),
    nearest_neighbor_matching_all=Input(rid="ri.foundry.main.dataset.7ed3e283-f6c6-43f0-baf6-ee29ea81c982"),
    prediction_dataset_bootstrapping_composite=Input(rid="ri.foundry.main.dataset.3c4d041c-a2d8-46b6-8382-8ccae890879b")
)
# Perform DTSA on each bootstrap (bb0b638d-c3c4-4655-b34d-d157d5aaa152): v1
def paxlovid_bootstrap_DTSA_composite(expanded_dataset_for_outcome_analysis_matching, prediction_dataset_bootstrapping_composite, nearest_neighbor_matching_all):
    prediction_dataset_bootstrapping_paxlovid = prediction_dataset_bootstrapping_composite

    import datetime
    import random
    from functools import reduce
    from pyspark.sql import DataFrame

    # This node will perform discrete time survival analysis to estimate the marginal survival curves for each treatment group; which are then used to estimate the relative risk of the outcome (hospitalization) on day 28

    # Steps:
    # 1. Take bootstrap
    # 2. Fit the propensity model and estimate the weights
    # 3. With the bootstrap - fit the DTSM
    # 4. Create prediction datasets 
    #   a. Prediction dataset for treated patients - use fully expanded dataset with ALL patients with treatment = 1
    #   b. Prediction dataset for control patients - use fully expanded dataset with ALL patients with treatment = 0
    # 5. Get predicted hazards in each dataset - under treated and under control
    # 6. Stack the datasets
    # 7. Average the hazards by day within each treatment condition = we will end up with 28 rows per treatment group
    # 8. Take the hazard complement (1 - H) and then calculate the survival curve as the cumulative product (make sure rows are sorted)
    # 9. We will end up with 28 rows per treatment group; with a "survival curve" column

    ################# NOW WE WILL FIT THE DTSA ###########################
    # Predictors are: treatment, day indicators, and the predictors
    df = expanded_dataset_for_outcome_analysis_matching
    # df = df.where(expr('trial <= 3'))

    time_end = 14
    time_indicator_features = ['d{}'.format(i) for i in np.arange(1, time_end+1)]
    treatment = 'treatment'
    outcome = 'outcome'
    id_column = 'person_id'
    time_column = 'time'
    matched_pair_column = 'subclass'
    
    # # Set up the outcome model features; and create our dataset for logistic regression (features + outcome)
    # predictors = ['treatment'] + time_indicator_features + features
    predictors = [treatment] + time_indicator_features
    # predictors_no_treatment = time_indicator_features + features
    predictors_no_treatment = time_indicator_features
    predictors_plus_outcome = predictors + [outcome]
    data_subset = df.select([matched_pair_column] + predictors_plus_outcome)

    ############# INCLUDE INTERACTIONS BETWEEN TREATMENT AND TIME INDICATORS ######
    # When we include this product - we do NOT include treatment as a predictor in the model ##### This will give us Tmt * Time, and Tmt * Predictors
    time_dependent_features = []
    for pred in predictors_no_treatment:
        data_subset = data_subset.withColumn('{treatment}_x_{predictor}'.format(treatment = treatment, predictor = pred), expr('{treatment} * {predictor}'.format(treatment = treatment, predictor = pred)))
        time_dependent_features.append('{treatment}_x_{predictor}'.format(treatment = treatment, predictor = pred))

    # # Calculate interactions between each time indicator AND each predictor. This will give us Time * Predictors
    # time_dependent_interactions = []
    # for time_indicator in time_indicator_features:
    #     for feature in features:
    #         data_subset = data_subset.withColumn('{}_x_{}'.format(time_indicator, feature), expr('{} * {}'.format(time_indicator, feature)))
    #         time_dependent_interactions.append('{}_x_{}'.format(time_indicator, feature))

    # Add the interactions between tmt * time indicators to the predictors object
    predictors = time_indicator_features + time_dependent_features
    print('final features:', predictors)

    ###### SET UP THE SAME FOR THE PREDICTION DATASET ###########
    # Set up the prediction df - this only contains the predictors. Calculate the interactions between tmt * time as above. 
    prediction_df = prediction_dataset_bootstrapping_paxlovid
    for pred in predictors_no_treatment:
        prediction_df = prediction_df.withColumn('{treatment}_x_{predictor}'.format(treatment = treatment, predictor = pred), expr('{treatment} * {predictor}'.format(treatment = treatment, predictor = pred)))
        
    # for time_indicator in time_indicator_features:
    #     for feature in features:
    #         prediction_df = prediction_df.withColumn('{}_x_{}'.format(time_indicator, feature), expr('{} * {}'.format(time_indicator, feature)))
    
    ####### SPARK ML LOGISTIC REGRESSION MODEL #######################
    from pyspark.sql import SparkSession
    from pyspark import SparkFiles
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
    from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
    from pyspark.ml.feature import VectorAssembler

    ############ PERFORM BOOTSTRAPPING ##########################
    # Identify the unique subclasses - we will bootstrap the subclasses
    unique_subclasses = df.select('subclass').distinct()
    n_unique_subclasses = unique_subclasses.count()

    # Create empty list to store bootstrap output

    Output_Prediction_DataFrames = []
    
    # Now, perform the bootstrapping
    for i in np.arange(0, 300):

        # First - get a sample of the subclasses with replacement
        random.seed(a = i)
        sampled_subclasses_df = unique_subclasses.sample(fraction=1.0, seed=i, withReplacement=True)

        # Use the subclass dataframe to filter the original dataframe (randomly select matched pairs)
        data_subset_sample = sampled_subclasses_df.join(data_subset, on = 'subclass', how = 'inner')

        # Set up the vector assembler. The "predictors" object is a list containing the treatment, time indicators, and treatment * time indicators
        assembler = VectorAssembler(inputCols = predictors, outputCol = 'predictors')
        data_subset_sample = assembler.transform(data_subset_sample)
        prediction_subset = assembler.transform(prediction_df) # Transform the prediction_df also; 

        # Set up the LogisticRegression
        logistic_regression = LogisticRegression(featuresCol = 'predictors', 
        labelCol = outcome, 
        family = 'binomial', 
        maxIter = 1000, 
        regParam = 0.0, 
        elasticNetParam = 1.0,
        fitIntercept=False,
        # weightCol = 'SW'
        )

        # Fit the DTSA model to the data
        model = logistic_regression.fit(data_subset_sample)

        # # Print out the coefficients
        # coefficients = model.coefficients
        # intercept = model.intercept
        # print("Coefficients: ", coefficients)
        # print("Intercept: {:.3f}".format(intercept))

        # Get the predictions; Function below extracts the probability of the outcome (= 1)
        def ith_(v, i):
            try:
                return float(v[i])
            except ValueError:
                return None

        ith = udf(ith_, DoubleType())

        # Get predictions for the prediction data. We want the treatment column, day column, and the hazard probability 
        denominator_predictions = model.transform(prediction_subset).select([treatment, time_column] + [ith(col('probability'), lit(1)).alias('hazard')])
        denominator_predictions = denominator_predictions.withColumn('bootstrap', lit('{}'.format(i)))

        # A. Now get the survival function 
        denominator_predictions = denominator_predictions.withColumn('survival', expr('1 - hazard'))
        
        # Calculate the cumulative product of the hazard complements (the conditional survival probability) within each treatment. This is already grouped by treatment and time. 
        window = Window.partitionBy([treatment]).orderBy(time_column).rowsBetween(Window.unboundedPreceding,Window.currentRow)

        # Add the bootstrap column 
        denominator_predictions = denominator_predictions.withColumn('survival', product(col('survival')).over(window))

        # Append the prediction Spark dataframe to our list
        Output_Prediction_DataFrames.append(denominator_predictions)

        print('bootstrap {} completed'.format(i))

    ################ FIT THE MODEL IN THE FULL DATAT TO GET THE POINT ESTIMATE ##############################################

    #### SET UP ELEMENTS FOR SPARK LR
    assembler = VectorAssembler(inputCols = predictors, outputCol = 'predictors')

    # Transform the input dataset (optional) and the prediction dataset
    data_subset_sample = assembler.transform(data_subset)
    prediction_subset = assembler.transform(prediction_df)  

    # Set up the LogisticRegression; we are not using weights
    logistic_regression = LogisticRegression(featuresCol = 'predictors', 
    labelCol = outcome, 
    family = 'binomial', 
    maxIter = 1000, 
    regParam = 0.0, 
    elasticNetParam = 1.0,
    fitIntercept=False,
    # weightCol = weight_column,
    )

    # Fit the model to the data
    model = logistic_regression.fit(data_subset_sample)

    # # Print out the coefficients
    # coefficients = model.coefficients
    # intercept = model.intercept
    # print("Coefficients: ", coefficients)
    # print("Intercept: {:.3f}".format(intercept))

    # Get the predictions; Function below extracts the probability of the outcome (= 1)
    def ith_(v, i):
        try:
            return float(v[i])
        except ValueError:
            return None

    ith = udf(ith_, DoubleType())

    # Get predictions for the prediction data. We want the treatment column, day column, and the hazard probability 
    prediction_output_df = model.transform(prediction_subset).select([treatment, time_column] + [ith(col('probability'), lit(1)).alias('hazard')])
    prediction_output_df = prediction_output_df.withColumn('bootstrap', lit(999))

    # Get the survival function for each patient BEFORE averaging that
    prediction_output_df = prediction_output_df.withColumn('survival', expr('1 - hazard'))
    
    # Calculate cumulative product of conditional survival probability (within each treatment)
    window = Window.partitionBy([treatment]).orderBy(time_column).rowsBetween(Window.unboundedPreceding,Window.currentRow)
    prediction_output_df = prediction_output_df.withColumn('survival', product(col('survival')).over(window))

    # Append the point estimate dataframe to our list
    Output_Prediction_DataFrames.append(prediction_output_df)
    

    ##################### FINALLY CONCATENATE ALL THE PREDICTIONS OUTPUTS INTO A SINGLE DATA FRAME ####################
    denominator_predictions_stacked = reduce(DataFrame.unionAll, Output_Prediction_DataFrames)

    return denominator_predictions_stacked
    
    

#################################################
## Global imports and functions included below ##
#################################################

######## GLOBAL CODE
# We can add this code to the GLOBAL CODE on the RHS pane
# Import the data types we will be using to functions and schemas
from pyspark.sql.types import StructType, DateType, StructField, StringType, IntegerType, FloatType, DoubleType
from pyspark.sql import Row

# Import functions and tools to make functions, and interact with columns with dot functions or SQL functions
from pyspark.sql.functions import lower, upper, col, udf, monotonically_increasing_id, to_date, trim, ltrim, rtrim, avg
from pyspark.sql.functions import length, size, unix_timestamp, from_unixtime, broadcast, to_timestamp, split, when, rand, count, round, countDistinct, product

# Additional Functions
from pyspark.sql.functions import min, max, col, mean, lit, sum, when, regexp_replace, lower, upper, concat_ws, to_date, floor, months_between, datediff, date_add, current_date, least, greatest, last_day, last, expr

# Functions for window functions
from pyspark.sql import Window
from pyspark.sql.functions import row_number

### PYTHON Functions
import datetime as dt

## GLOBAL PY CODE
### Import all necessary packages
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re

# Pandas functions
idx = pd.IndexSlice

# Viewing related functions
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a81d7b76-0066-4fc5-a522-5fdaf39b5128"),
    expanded_dataset_for_outcome_analysis_matching_compositedeathhosp=Input(rid="ri.foundry.main.dataset.1b4818d5-123b-4f59-a7b8-a341d2330a85"),
    paxlovid_bootstrap_DTSA_modcomposite=Input(rid="ri.foundry.main.dataset.79d27717-1ad0-4c1a-9590-cc4beda25648"),
    prediction_dataset_bootstrapping_compositedeathhosp=Input(rid="ri.foundry.main.dataset.fe128665-5d79-4cbb-9fad-a776395fa0a6")
)
# Perform DTSA on each bootstrap (bb0b638d-c3c4-4655-b34d-d157d5aaa152): v1
def paxlovid_bootstrap_DTSA_compositedeathhosp(expanded_dataset_for_outcome_analysis_matching_compositedeathhosp, prediction_dataset_bootstrapping_compositedeathhosp, paxlovid_bootstrap_DTSA_modcomposite):
    prediction_dataset_bootstrapping_paxlovid = prediction_dataset_bootstrapping_compositedeathhosp

    import datetime
    import random
    from functools import reduce
    from pyspark.sql import DataFrame

    # This node will perform discrete time survival analysis to estimate the marginal survival curves for each treatment group; which are then used to estimate the relative risk of the outcome (hospitalization) on day 28

    # Steps:
    # 1. Take bootstrap
    # 2. Fit the propensity model and estimate the weights
    # 3. With the bootstrap - fit the DTSM
    # 4. Create prediction datasets 
    #   a. Prediction dataset for treated patients - use fully expanded dataset with ALL patients with treatment = 1
    #   b. Prediction dataset for control patients - use fully expanded dataset with ALL patients with treatment = 0
    # 5. Get predicted hazards in each dataset - under treated and under control
    # 6. Stack the datasets
    # 7. Average the hazards by day within each treatment condition = we will end up with 28 rows per treatment group
    # 8. Take the hazard complement (1 - H) and then calculate the survival curve as the cumulative product (make sure rows are sorted)
    # 9. We will end up with 28 rows per treatment group; with a "survival curve" column

    ################# NOW WE WILL FIT THE DTSA ###########################
    # Predictors are: treatment, day indicators, and the predictors
    df = expanded_dataset_for_outcome_analysis_matching_compositedeathhosp
    # df = df.where(expr('trial <= 3'))

    time_end = 14
    time_indicator_features = ['d{}'.format(i) for i in np.arange(1, time_end+1)]
    treatment = 'treatment'
    outcome = 'outcome'
    id_column = 'person_id'
    time_column = 'time'
    matched_pair_column = 'subclass'
    
    # # Set up the outcome model features; and create our dataset for logistic regression (features + outcome)
    # predictors = ['treatment'] + time_indicator_features + features
    predictors = [treatment] + time_indicator_features
    # predictors_no_treatment = time_indicator_features + features
    predictors_no_treatment = time_indicator_features
    predictors_plus_outcome = predictors + [outcome]
    data_subset = df.select([matched_pair_column] + predictors_plus_outcome)

    ############# INCLUDE INTERACTIONS BETWEEN TREATMENT AND TIME INDICATORS ######
    # When we include this product - we do NOT include treatment as a predictor in the model ##### This will give us Tmt * Time, and Tmt * Predictors
    time_dependent_features = []
    for pred in predictors_no_treatment:
        data_subset = data_subset.withColumn('{treatment}_x_{predictor}'.format(treatment = treatment, predictor = pred), expr('{treatment} * {predictor}'.format(treatment = treatment, predictor = pred)))
        time_dependent_features.append('{treatment}_x_{predictor}'.format(treatment = treatment, predictor = pred))

    # # Calculate interactions between each time indicator AND each predictor. This will give us Time * Predictors
    # time_dependent_interactions = []
    # for time_indicator in time_indicator_features:
    #     for feature in features:
    #         data_subset = data_subset.withColumn('{}_x_{}'.format(time_indicator, feature), expr('{} * {}'.format(time_indicator, feature)))
    #         time_dependent_interactions.append('{}_x_{}'.format(time_indicator, feature))

    # Add the interactions between tmt * time indicators to the predictors object
    predictors = time_indicator_features + time_dependent_features
    print('final features:', predictors)

    ###### SET UP THE SAME FOR THE PREDICTION DATASET ###########
    # Set up the prediction df - this only contains the predictors. Calculate the interactions between tmt * time as above. 
    prediction_df = prediction_dataset_bootstrapping_paxlovid
    for pred in predictors_no_treatment:
        prediction_df = prediction_df.withColumn('{treatment}_x_{predictor}'.format(treatment = treatment, predictor = pred), expr('{treatment} * {predictor}'.format(treatment = treatment, predictor = pred)))
        
    # for time_indicator in time_indicator_features:
    #     for feature in features:
    #         prediction_df = prediction_df.withColumn('{}_x_{}'.format(time_indicator, feature), expr('{} * {}'.format(time_indicator, feature)))
    
    ####### SPARK ML LOGISTIC REGRESSION MODEL #######################
    from pyspark.sql import SparkSession
    from pyspark import SparkFiles
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
    from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
    from pyspark.ml.feature import VectorAssembler

    ############ PERFORM BOOTSTRAPPING ##########################
    # Identify the unique subclasses - we will bootstrap the subclasses
    unique_subclasses = df.select('subclass').distinct()
    n_unique_subclasses = unique_subclasses.count()

    # Create empty list to store bootstrap output

    Output_Prediction_DataFrames = []
    
    # Now, perform the bootstrapping
    for i in np.arange(0, 300):

        # First - get a sample of the subclasses with replacement
        random.seed(a = i)
        sampled_subclasses_df = unique_subclasses.sample(fraction=1.0, seed=i, withReplacement=True)

        # Use the subclass dataframe to filter the original dataframe (randomly select matched pairs)
        data_subset_sample = sampled_subclasses_df.join(data_subset, on = 'subclass', how = 'inner')

        # Set up the vector assembler. The "predictors" object is a list containing the treatment, time indicators, and treatment * time indicators
        assembler = VectorAssembler(inputCols = predictors, outputCol = 'predictors')
        data_subset_sample = assembler.transform(data_subset_sample)
        prediction_subset = assembler.transform(prediction_df) # Transform the prediction_df also; 

        # Set up the LogisticRegression
        logistic_regression = LogisticRegression(featuresCol = 'predictors', 
        labelCol = outcome, 
        family = 'binomial', 
        maxIter = 1000, 
        regParam = 0.0, 
        elasticNetParam = 1.0,
        fitIntercept=False,
        # weightCol = 'SW'
        )

        # Fit the DTSA model to the data
        model = logistic_regression.fit(data_subset_sample)

        # # Print out the coefficients
        # coefficients = model.coefficients
        # intercept = model.intercept
        # print("Coefficients: ", coefficients)
        # print("Intercept: {:.3f}".format(intercept))

        # Get the predictions; Function below extracts the probability of the outcome (= 1)
        def ith_(v, i):
            try:
                return float(v[i])
            except ValueError:
                return None

        ith = udf(ith_, DoubleType())

        # Get predictions for the prediction data. We want the treatment column, day column, and the hazard probability 
        denominator_predictions = model.transform(prediction_subset).select([treatment, time_column] + [ith(col('probability'), lit(1)).alias('hazard')])
        denominator_predictions = denominator_predictions.withColumn('bootstrap', lit('{}'.format(i)))

        # A. Now get the survival function 
        denominator_predictions = denominator_predictions.withColumn('survival', expr('1 - hazard'))
        
        # Calculate the cumulative product of the hazard complements (the conditional survival probability) within each treatment. This is already grouped by treatment and time. 
        window = Window.partitionBy([treatment]).orderBy(time_column).rowsBetween(Window.unboundedPreceding,Window.currentRow)

        # Add the bootstrap column 
        denominator_predictions = denominator_predictions.withColumn('survival', product(col('survival')).over(window))

        # Append the prediction Spark dataframe to our list
        Output_Prediction_DataFrames.append(denominator_predictions)

        print('bootstrap {} completed'.format(i))

    ################ FIT THE MODEL IN THE FULL DATAT TO GET THE POINT ESTIMATE ##############################################

    #### SET UP ELEMENTS FOR SPARK LR
    assembler = VectorAssembler(inputCols = predictors, outputCol = 'predictors')

    # Transform the input dataset (optional) and the prediction dataset
    data_subset_sample = assembler.transform(data_subset)
    prediction_subset = assembler.transform(prediction_df)  

    # Set up the LogisticRegression; we are not using weights
    logistic_regression = LogisticRegression(featuresCol = 'predictors', 
    labelCol = outcome, 
    family = 'binomial', 
    maxIter = 1000, 
    regParam = 0.0, 
    elasticNetParam = 1.0,
    fitIntercept=False,
    # weightCol = weight_column,
    )

    # Fit the model to the data
    model = logistic_regression.fit(data_subset_sample)

    # # Print out the coefficients
    # coefficients = model.coefficients
    # intercept = model.intercept
    # print("Coefficients: ", coefficients)
    # print("Intercept: {:.3f}".format(intercept))

    # Get the predictions; Function below extracts the probability of the outcome (= 1)
    def ith_(v, i):
        try:
            return float(v[i])
        except ValueError:
            return None

    ith = udf(ith_, DoubleType())

    # Get predictions for the prediction data. We want the treatment column, day column, and the hazard probability 
    prediction_output_df = model.transform(prediction_subset).select([treatment, time_column] + [ith(col('probability'), lit(1)).alias('hazard')])
    prediction_output_df = prediction_output_df.withColumn('bootstrap', lit(999))

    # Get the survival function for each patient BEFORE averaging that
    prediction_output_df = prediction_output_df.withColumn('survival', expr('1 - hazard'))
    
    # Calculate cumulative product of conditional survival probability (within each treatment)
    window = Window.partitionBy([treatment]).orderBy(time_column).rowsBetween(Window.unboundedPreceding,Window.currentRow)
    prediction_output_df = prediction_output_df.withColumn('survival', product(col('survival')).over(window))

    # Append the point estimate dataframe to our list
    Output_Prediction_DataFrames.append(prediction_output_df)
    

    ##################### FINALLY CONCATENATE ALL THE PREDICTIONS OUTPUTS INTO A SINGLE DATA FRAME ####################
    denominator_predictions_stacked = reduce(DataFrame.unionAll, Output_Prediction_DataFrames)

    return denominator_predictions_stacked
    
    

#################################################
## Global imports and functions included below ##
#################################################

######## GLOBAL CODE
# We can add this code to the GLOBAL CODE on the RHS pane
# Import the data types we will be using to functions and schemas
from pyspark.sql.types import StructType, DateType, StructField, StringType, IntegerType, FloatType, DoubleType
from pyspark.sql import Row

# Import functions and tools to make functions, and interact with columns with dot functions or SQL functions
from pyspark.sql.functions import lower, upper, col, udf, monotonically_increasing_id, to_date, trim, ltrim, rtrim, avg
from pyspark.sql.functions import length, size, unix_timestamp, from_unixtime, broadcast, to_timestamp, split, when, rand, count, round, countDistinct, product

# Additional Functions
from pyspark.sql.functions import min, max, col, mean, lit, sum, when, regexp_replace, lower, upper, concat_ws, to_date, floor, months_between, datediff, date_add, current_date, least, greatest, last_day, last, expr

# Functions for window functions
from pyspark.sql import Window
from pyspark.sql.functions import row_number

### PYTHON Functions
import datetime as dt

## GLOBAL PY CODE
### Import all necessary packages
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re

# Pandas functions
idx = pd.IndexSlice

# Viewing related functions
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3993bec5-34a6-47d9-b910-6698d1c77119"),
    expanded_dataset_for_outcome_analysis_matching_deathoutcome=Input(rid="ri.foundry.main.dataset.7fcd678b-62da-48b1-90e6-1c7e11ec5065"),
    paxlovid_bootstrap_DTSA_compositedeathhosp=Input(rid="ri.foundry.main.dataset.a81d7b76-0066-4fc5-a522-5fdaf39b5128"),
    prediction_dataset_bootstrapping_death=Input(rid="ri.foundry.main.dataset.243ad79a-ef32-4cde-9400-9c8ae7b35c81")
)
# Perform DTSA on each bootstrap (bb0b638d-c3c4-4655-b34d-d157d5aaa152): v1
def paxlovid_bootstrap_DTSA_death(expanded_dataset_for_outcome_analysis_matching_deathoutcome, prediction_dataset_bootstrapping_death, paxlovid_bootstrap_DTSA_compositedeathhosp):
    prediction_dataset_bootstrapping_paxlovid = prediction_dataset_bootstrapping_death

    import datetime
    import random
    from functools import reduce
    from pyspark.sql import DataFrame

    # This node will perform discrete time survival analysis to estimate the marginal survival curves for each treatment group; which are then used to estimate the relative risk of the outcome (hospitalization) on day 28

    # Steps:
    # 1. Take bootstrap
    # 2. Fit the propensity model and estimate the weights
    # 3. With the bootstrap - fit the DTSM
    # 4. Create prediction datasets 
    #   a. Prediction dataset for treated patients - use fully expanded dataset with ALL patients with treatment = 1
    #   b. Prediction dataset for control patients - use fully expanded dataset with ALL patients with treatment = 0
    # 5. Get predicted hazards in each dataset - under treated and under control
    # 6. Stack the datasets
    # 7. Average the hazards by day within each treatment condition = we will end up with 28 rows per treatment group
    # 8. Take the hazard complement (1 - H) and then calculate the survival curve as the cumulative product (make sure rows are sorted)
    # 9. We will end up with 28 rows per treatment group; with a "survival curve" column

    ################# NOW WE WILL FIT THE DTSA ###########################
    # Predictors are: treatment, day indicators, and the predictors
    df = expanded_dataset_for_outcome_analysis_matching_deathoutcome
    # df = df.where(expr('trial <= 3'))

    time_end = 14
    time_indicator_features = ['d{}'.format(i) for i in np.arange(1, time_end+1)]
    treatment = 'treatment'
    outcome = 'outcome'
    id_column = 'person_id'
    time_column = 'time'
    matched_pair_column = 'subclass'
    
    # # Set up the outcome model features; and create our dataset for logistic regression (features + outcome)
    # predictors = ['treatment'] + time_indicator_features + features
    predictors = [treatment] + time_indicator_features
    # predictors_no_treatment = time_indicator_features + features
    predictors_no_treatment = time_indicator_features
    predictors_plus_outcome = predictors + [outcome]
    data_subset = df.select([matched_pair_column] + predictors_plus_outcome)

    ############# INCLUDE INTERACTIONS BETWEEN TREATMENT AND TIME INDICATORS ######
    # When we include this product - we do NOT include treatment as a predictor in the model ##### This will give us Tmt * Time, and Tmt * Predictors
    time_dependent_features = []
    for pred in predictors_no_treatment:
        data_subset = data_subset.withColumn('{treatment}_x_{predictor}'.format(treatment = treatment, predictor = pred), expr('{treatment} * {predictor}'.format(treatment = treatment, predictor = pred)))
        time_dependent_features.append('{treatment}_x_{predictor}'.format(treatment = treatment, predictor = pred))

    # # Calculate interactions between each time indicator AND each predictor. This will give us Time * Predictors
    # time_dependent_interactions = []
    # for time_indicator in time_indicator_features:
    #     for feature in features:
    #         data_subset = data_subset.withColumn('{}_x_{}'.format(time_indicator, feature), expr('{} * {}'.format(time_indicator, feature)))
    #         time_dependent_interactions.append('{}_x_{}'.format(time_indicator, feature))

    # Add the interactions between tmt * time indicators to the predictors object
    predictors = time_indicator_features + time_dependent_features
    print('final features:', predictors)

    ###### SET UP THE SAME FOR THE PREDICTION DATASET ###########
    # Set up the prediction df - this only contains the predictors. Calculate the interactions between tmt * time as above. 
    prediction_df = prediction_dataset_bootstrapping_paxlovid
    for pred in predictors_no_treatment:
        prediction_df = prediction_df.withColumn('{treatment}_x_{predictor}'.format(treatment = treatment, predictor = pred), expr('{treatment} * {predictor}'.format(treatment = treatment, predictor = pred)))
        
    # for time_indicator in time_indicator_features:
    #     for feature in features:
    #         prediction_df = prediction_df.withColumn('{}_x_{}'.format(time_indicator, feature), expr('{} * {}'.format(time_indicator, feature)))
    
    ####### SPARK ML LOGISTIC REGRESSION MODEL #######################
    from pyspark.sql import SparkSession
    from pyspark import SparkFiles
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
    from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
    from pyspark.ml.feature import VectorAssembler

    ############ PERFORM BOOTSTRAPPING ##########################
    # Identify the unique subclasses - we will bootstrap the subclasses
    unique_subclasses = df.select('subclass').distinct()
    n_unique_subclasses = unique_subclasses.count()

    # Create empty list to store bootstrap output

    Output_Prediction_DataFrames = []
    
    # Now, perform the bootstrapping
    for i in np.arange(0, 300):

        # First - get a sample of the subclasses with replacement
        random.seed(a = i)
        sampled_subclasses_df = unique_subclasses.sample(fraction=1.0, seed=i, withReplacement=True)

        # Use the subclass dataframe to filter the original dataframe (randomly select matched pairs)
        data_subset_sample = sampled_subclasses_df.join(data_subset, on = 'subclass', how = 'inner')

        # Set up the vector assembler. The "predictors" object is a list containing the treatment, time indicators, and treatment * time indicators
        assembler = VectorAssembler(inputCols = predictors, outputCol = 'predictors')
        data_subset_sample = assembler.transform(data_subset_sample)
        prediction_subset = assembler.transform(prediction_df) # Transform the prediction_df also; 

        # Set up the LogisticRegression
        logistic_regression = LogisticRegression(featuresCol = 'predictors', 
        labelCol = outcome, 
        family = 'binomial', 
        maxIter = 1000, 
        regParam = 0.0, 
        elasticNetParam = 1.0,
        fitIntercept=False,
        # weightCol = 'SW'
        )

        # Fit the DTSA model to the data
        model = logistic_regression.fit(data_subset_sample)

        # # Print out the coefficients
        # coefficients = model.coefficients
        # intercept = model.intercept
        # print("Coefficients: ", coefficients)
        # print("Intercept: {:.3f}".format(intercept))

        # Get the predictions; Function below extracts the probability of the outcome (= 1)
        def ith_(v, i):
            try:
                return float(v[i])
            except ValueError:
                return None

        ith = udf(ith_, DoubleType())

        # Get predictions for the prediction data. We want the treatment column, day column, and the hazard probability 
        denominator_predictions = model.transform(prediction_subset).select([treatment, time_column] + [ith(col('probability'), lit(1)).alias('hazard')])
        denominator_predictions = denominator_predictions.withColumn('bootstrap', lit('{}'.format(i)))

        # A. Now get the survival function 
        denominator_predictions = denominator_predictions.withColumn('survival', expr('1 - hazard'))
        
        # Calculate the cumulative product of the hazard complements (the conditional survival probability) within each treatment. This is already grouped by treatment and time. 
        window = Window.partitionBy([treatment]).orderBy(time_column).rowsBetween(Window.unboundedPreceding,Window.currentRow)

        # Add the bootstrap column 
        denominator_predictions = denominator_predictions.withColumn('survival', product(col('survival')).over(window))

        # Append the prediction Spark dataframe to our list
        Output_Prediction_DataFrames.append(denominator_predictions)

        print('bootstrap {} completed'.format(i))

    ################ FIT THE MODEL IN THE FULL DATAT TO GET THE POINT ESTIMATE ##############################################

    #### SET UP ELEMENTS FOR SPARK LR
    assembler = VectorAssembler(inputCols = predictors, outputCol = 'predictors')

    # Transform the input dataset (optional) and the prediction dataset
    data_subset_sample = assembler.transform(data_subset)
    prediction_subset = assembler.transform(prediction_df)  

    # Set up the LogisticRegression; we are not using weights
    logistic_regression = LogisticRegression(featuresCol = 'predictors', 
    labelCol = outcome, 
    family = 'binomial', 
    maxIter = 1000, 
    regParam = 0.0, 
    elasticNetParam = 1.0,
    fitIntercept=False,
    # weightCol = weight_column,
    )

    # Fit the model to the data
    model = logistic_regression.fit(data_subset_sample)

    # # Print out the coefficients
    # coefficients = model.coefficients
    # intercept = model.intercept
    # print("Coefficients: ", coefficients)
    # print("Intercept: {:.3f}".format(intercept))

    # Get the predictions; Function below extracts the probability of the outcome (= 1)
    def ith_(v, i):
        try:
            return float(v[i])
        except ValueError:
            return None

    ith = udf(ith_, DoubleType())

    # Get predictions for the prediction data. We want the treatment column, day column, and the hazard probability 
    prediction_output_df = model.transform(prediction_subset).select([treatment, time_column] + [ith(col('probability'), lit(1)).alias('hazard')])
    prediction_output_df = prediction_output_df.withColumn('bootstrap', lit(999))

    # Get the survival function for each patient BEFORE averaging that
    prediction_output_df = prediction_output_df.withColumn('survival', expr('1 - hazard'))
    
    # Calculate cumulative product of conditional survival probability (within each treatment)
    window = Window.partitionBy([treatment]).orderBy(time_column).rowsBetween(Window.unboundedPreceding,Window.currentRow)
    prediction_output_df = prediction_output_df.withColumn('survival', product(col('survival')).over(window))

    # Append the point estimate dataframe to our list
    Output_Prediction_DataFrames.append(prediction_output_df)
    

    ##################### FINALLY CONCATENATE ALL THE PREDICTIONS OUTPUTS INTO A SINGLE DATA FRAME ####################
    denominator_predictions_stacked = reduce(DataFrame.unionAll, Output_Prediction_DataFrames)

    return denominator_predictions_stacked
    
    

#################################################
## Global imports and functions included below ##
#################################################

######## GLOBAL CODE
# We can add this code to the GLOBAL CODE on the RHS pane
# Import the data types we will be using to functions and schemas
from pyspark.sql.types import StructType, DateType, StructField, StringType, IntegerType, FloatType, DoubleType
from pyspark.sql import Row

# Import functions and tools to make functions, and interact with columns with dot functions or SQL functions
from pyspark.sql.functions import lower, upper, col, udf, monotonically_increasing_id, to_date, trim, ltrim, rtrim, avg
from pyspark.sql.functions import length, size, unix_timestamp, from_unixtime, broadcast, to_timestamp, split, when, rand, count, round, countDistinct, product

# Additional Functions
from pyspark.sql.functions import min, max, col, mean, lit, sum, when, regexp_replace, lower, upper, concat_ws, to_date, floor, months_between, datediff, date_add, current_date, least, greatest, last_day, last, expr

# Functions for window functions
from pyspark.sql import Window
from pyspark.sql.functions import row_number

### PYTHON Functions
import datetime as dt

## GLOBAL PY CODE
### Import all necessary packages
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re

# Pandas functions
idx = pd.IndexSlice

# Viewing related functions
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.79d27717-1ad0-4c1a-9590-cc4beda25648"),
    expanded_dataset_for_outcome_analysis_matching_modcomposite=Input(rid="ri.foundry.main.dataset.1a14ee76-280e-46a0-8072-d3adea949e40"),
    paxlovid_bootstrap_DTSA_composite=Input(rid="ri.foundry.main.dataset.b8ad3326-707a-4073-9e8f-0c8317bdfb2e"),
    prediction_dataset_bootstrapping_modcomposite=Input(rid="ri.foundry.main.dataset.b9c72c59-ef49-4bfc-b5d1-f00ddc4a4be1")
)
# Perform DTSA on each bootstrap (bb0b638d-c3c4-4655-b34d-d157d5aaa152): v1
def paxlovid_bootstrap_DTSA_modcomposite(expanded_dataset_for_outcome_analysis_matching_modcomposite, prediction_dataset_bootstrapping_modcomposite, paxlovid_bootstrap_DTSA_composite):
    prediction_dataset_bootstrapping_paxlovid = prediction_dataset_bootstrapping_modcomposite

    import datetime
    import random
    from functools import reduce
    from pyspark.sql import DataFrame

    # This node will perform discrete time survival analysis to estimate the marginal survival curves for each treatment group; which are then used to estimate the relative risk of the outcome (hospitalization) on day 28

    # Steps:
    # 1. Take bootstrap
    # 2. Fit the propensity model and estimate the weights
    # 3. With the bootstrap - fit the DTSM
    # 4. Create prediction datasets 
    #   a. Prediction dataset for treated patients - use fully expanded dataset with ALL patients with treatment = 1
    #   b. Prediction dataset for control patients - use fully expanded dataset with ALL patients with treatment = 0
    # 5. Get predicted hazards in each dataset - under treated and under control
    # 6. Stack the datasets
    # 7. Average the hazards by day within each treatment condition = we will end up with 28 rows per treatment group
    # 8. Take the hazard complement (1 - H) and then calculate the survival curve as the cumulative product (make sure rows are sorted)
    # 9. We will end up with 28 rows per treatment group; with a "survival curve" column

    ################# NOW WE WILL FIT THE DTSA ###########################
    # Predictors are: treatment, day indicators, and the predictors
    df = expanded_dataset_for_outcome_analysis_matching_modcomposite
    # df = df.where(expr('trial <= 3'))

    time_end = 14
    time_indicator_features = ['d{}'.format(i) for i in np.arange(1, time_end+1)]
    treatment = 'treatment'
    outcome = 'outcome'
    id_column = 'person_id'
    time_column = 'time'
    matched_pair_column = 'subclass'
    
    # # Set up the outcome model features; and create our dataset for logistic regression (features + outcome)
    # predictors = ['treatment'] + time_indicator_features + features
    predictors = [treatment] + time_indicator_features
    # predictors_no_treatment = time_indicator_features + features
    predictors_no_treatment = time_indicator_features
    predictors_plus_outcome = predictors + [outcome]
    data_subset = df.select([matched_pair_column] + predictors_plus_outcome)

    ############# INCLUDE INTERACTIONS BETWEEN TREATMENT AND TIME INDICATORS ######
    # When we include this product - we do NOT include treatment as a predictor in the model ##### This will give us Tmt * Time, and Tmt * Predictors
    time_dependent_features = []
    for pred in predictors_no_treatment:
        data_subset = data_subset.withColumn('{treatment}_x_{predictor}'.format(treatment = treatment, predictor = pred), expr('{treatment} * {predictor}'.format(treatment = treatment, predictor = pred)))
        time_dependent_features.append('{treatment}_x_{predictor}'.format(treatment = treatment, predictor = pred))

    # # Calculate interactions between each time indicator AND each predictor. This will give us Time * Predictors
    # time_dependent_interactions = []
    # for time_indicator in time_indicator_features:
    #     for feature in features:
    #         data_subset = data_subset.withColumn('{}_x_{}'.format(time_indicator, feature), expr('{} * {}'.format(time_indicator, feature)))
    #         time_dependent_interactions.append('{}_x_{}'.format(time_indicator, feature))

    # Add the interactions between tmt * time indicators to the predictors object
    predictors = time_indicator_features + time_dependent_features
    print('final features:', predictors)

    ###### SET UP THE SAME FOR THE PREDICTION DATASET ###########
    # Set up the prediction df - this only contains the predictors. Calculate the interactions between tmt * time as above. 
    prediction_df = prediction_dataset_bootstrapping_paxlovid
    for pred in predictors_no_treatment:
        prediction_df = prediction_df.withColumn('{treatment}_x_{predictor}'.format(treatment = treatment, predictor = pred), expr('{treatment} * {predictor}'.format(treatment = treatment, predictor = pred)))
        
    # for time_indicator in time_indicator_features:
    #     for feature in features:
    #         prediction_df = prediction_df.withColumn('{}_x_{}'.format(time_indicator, feature), expr('{} * {}'.format(time_indicator, feature)))
    
    ####### SPARK ML LOGISTIC REGRESSION MODEL #######################
    from pyspark.sql import SparkSession
    from pyspark import SparkFiles
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
    from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
    from pyspark.ml.feature import VectorAssembler

    ############ PERFORM BOOTSTRAPPING ##########################
    # Identify the unique subclasses - we will bootstrap the subclasses
    unique_subclasses = df.select('subclass').distinct()
    n_unique_subclasses = unique_subclasses.count()

    # Create empty list to store bootstrap output

    Output_Prediction_DataFrames = []
    
    # Now, perform the bootstrapping
    for i in np.arange(0, 300):

        # First - get a sample of the subclasses with replacement
        random.seed(a = i)
        sampled_subclasses_df = unique_subclasses.sample(fraction=1.0, seed=i, withReplacement=True)

        # Use the subclass dataframe to filter the original dataframe (randomly select matched pairs)
        data_subset_sample = sampled_subclasses_df.join(data_subset, on = 'subclass', how = 'inner')

        # Set up the vector assembler. The "predictors" object is a list containing the treatment, time indicators, and treatment * time indicators
        assembler = VectorAssembler(inputCols = predictors, outputCol = 'predictors')
        data_subset_sample = assembler.transform(data_subset_sample)
        prediction_subset = assembler.transform(prediction_df) # Transform the prediction_df also; 

        # Set up the LogisticRegression
        logistic_regression = LogisticRegression(featuresCol = 'predictors', 
        labelCol = outcome, 
        family = 'binomial', 
        maxIter = 1000, 
        regParam = 0.0, 
        elasticNetParam = 1.0,
        fitIntercept=False,
        # weightCol = 'SW'
        )

        # Fit the DTSA model to the data
        model = logistic_regression.fit(data_subset_sample)

        # # Print out the coefficients
        # coefficients = model.coefficients
        # intercept = model.intercept
        # print("Coefficients: ", coefficients)
        # print("Intercept: {:.3f}".format(intercept))

        # Get the predictions; Function below extracts the probability of the outcome (= 1)
        def ith_(v, i):
            try:
                return float(v[i])
            except ValueError:
                return None

        ith = udf(ith_, DoubleType())

        # Get predictions for the prediction data. We want the treatment column, day column, and the hazard probability 
        denominator_predictions = model.transform(prediction_subset).select([treatment, time_column] + [ith(col('probability'), lit(1)).alias('hazard')])
        denominator_predictions = denominator_predictions.withColumn('bootstrap', lit('{}'.format(i)))

        # A. Now get the survival function 
        denominator_predictions = denominator_predictions.withColumn('survival', expr('1 - hazard'))
        
        # Calculate the cumulative product of the hazard complements (the conditional survival probability) within each treatment. This is already grouped by treatment and time. 
        window = Window.partitionBy([treatment]).orderBy(time_column).rowsBetween(Window.unboundedPreceding,Window.currentRow)

        # Add the bootstrap column 
        denominator_predictions = denominator_predictions.withColumn('survival', product(col('survival')).over(window))

        # Append the prediction Spark dataframe to our list
        Output_Prediction_DataFrames.append(denominator_predictions)

        print('bootstrap {} completed'.format(i))

    ################ FIT THE MODEL IN THE FULL DATAT TO GET THE POINT ESTIMATE ##############################################

    #### SET UP ELEMENTS FOR SPARK LR
    assembler = VectorAssembler(inputCols = predictors, outputCol = 'predictors')

    # Transform the input dataset (optional) and the prediction dataset
    data_subset_sample = assembler.transform(data_subset)
    prediction_subset = assembler.transform(prediction_df)  

    # Set up the LogisticRegression; we are not using weights
    logistic_regression = LogisticRegression(featuresCol = 'predictors', 
    labelCol = outcome, 
    family = 'binomial', 
    maxIter = 1000, 
    regParam = 0.0, 
    elasticNetParam = 1.0,
    fitIntercept=False,
    # weightCol = weight_column,
    )

    # Fit the model to the data
    model = logistic_regression.fit(data_subset_sample)

    # # Print out the coefficients
    # coefficients = model.coefficients
    # intercept = model.intercept
    # print("Coefficients: ", coefficients)
    # print("Intercept: {:.3f}".format(intercept))

    # Get the predictions; Function below extracts the probability of the outcome (= 1)
    def ith_(v, i):
        try:
            return float(v[i])
        except ValueError:
            return None

    ith = udf(ith_, DoubleType())

    # Get predictions for the prediction data. We want the treatment column, day column, and the hazard probability 
    prediction_output_df = model.transform(prediction_subset).select([treatment, time_column] + [ith(col('probability'), lit(1)).alias('hazard')])
    prediction_output_df = prediction_output_df.withColumn('bootstrap', lit(999))

    # Get the survival function for each patient BEFORE averaging that
    prediction_output_df = prediction_output_df.withColumn('survival', expr('1 - hazard'))
    
    # Calculate cumulative product of conditional survival probability (within each treatment)
    window = Window.partitionBy([treatment]).orderBy(time_column).rowsBetween(Window.unboundedPreceding,Window.currentRow)
    prediction_output_df = prediction_output_df.withColumn('survival', product(col('survival')).over(window))

    # Append the point estimate dataframe to our list
    Output_Prediction_DataFrames.append(prediction_output_df)
    

    ##################### FINALLY CONCATENATE ALL THE PREDICTIONS OUTPUTS INTO A SINGLE DATA FRAME ####################
    denominator_predictions_stacked = reduce(DataFrame.unionAll, Output_Prediction_DataFrames)

    return denominator_predictions_stacked
    
    

#################################################
## Global imports and functions included below ##
#################################################

######## GLOBAL CODE
# We can add this code to the GLOBAL CODE on the RHS pane
# Import the data types we will be using to functions and schemas
from pyspark.sql.types import StructType, DateType, StructField, StringType, IntegerType, FloatType, DoubleType
from pyspark.sql import Row

# Import functions and tools to make functions, and interact with columns with dot functions or SQL functions
from pyspark.sql.functions import lower, upper, col, udf, monotonically_increasing_id, to_date, trim, ltrim, rtrim, avg
from pyspark.sql.functions import length, size, unix_timestamp, from_unixtime, broadcast, to_timestamp, split, when, rand, count, round, countDistinct, product

# Additional Functions
from pyspark.sql.functions import min, max, col, mean, lit, sum, when, regexp_replace, lower, upper, concat_ws, to_date, floor, months_between, datediff, date_add, current_date, least, greatest, last_day, last, expr

# Functions for window functions
from pyspark.sql import Window
from pyspark.sql.functions import row_number

### PYTHON Functions
import datetime as dt

## GLOBAL PY CODE
### Import all necessary packages
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re

# Pandas functions
idx = pd.IndexSlice

# Viewing related functions
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3d2b0e13-8f89-43c2-b72a-f1e6cad12d67"),
    long_covid_date=Input(rid="ri.foundry.main.dataset.69dade5f-c43f-457a-89da-652a3e4a6e2a"),
    long_covid_date_v2=Input(rid="ri.foundry.main.dataset.5f0de766-9252-47b9-b981-c43c93a1181d"),
    nearest_neighbor_matching_LC=Input(rid="ri.foundry.main.dataset.6d3eb3b5-af0e-4831-9d10-c593c7158038"),
    propensity_model_prep_expanded=Input(rid="ri.foundry.main.dataset.9999bcdb-ba34-4487-99b3-64a79b95e279")
)
def paxlovid_expanded_data_long_covid( long_covid_date, long_covid_date_v2, nearest_neighbor_matching_LC, propensity_model_prep_expanded):

    # The purpose of this node is to expand the dataset for discrete time survival analysis; and to set up the dataset for Cox regression; 

    # import long covid table, change date to long_covid_date
    long_covid_df = long_covid_date_v2.withColumnRenamed('date','long_covid_date').withColumnRenamed('LONG_COVID','long_covid').select('person_id','long_covid_date','long_covid','window_start')

    # 1. Create a cross join between person id and days up to day 180; We add 70 to time end because - the final prediction window which occurs on month 6, ends 100 days after the start of the period.
    grace_period = 5
    months_max = 7
    time_end = 180 + 70
    follow_up_length = time_end
    outcome = 'long_covid'
    treatment_column = 'treatment'
    outcome_date = '{}_date'.format(outcome)
    outcome_columns = ['long_covid','long_covid_date','long_covid_time']
    
    # Create the cross join
    days = np.arange(1, time_end + 1)
    days_df = pd.DataFrame({'time': days})
    days_df = spark.createDataFrame(days_df)
    person_df = nearest_neighbor_matching_LC.select('person_id').distinct()
    full_join = person_df.join(days_df)
    
    # Join the cross join to matching output. In the matching output, each line is a person-trial. Each person-trial now gets joined to 180 days. 
    matching_output = nearest_neighbor_matching_LC.select('person_id','day','subclass','distance',treatment_column)
    df = matching_output.join(full_join, on = 'person_id', how = 'inner').withColumnRenamed('day','trial')

    # Get the trial start dates (date)
    trial_start_dates = propensity_model_prep_expanded.select('person_id','day','date','COVID_index_date','death_date').withColumnRenamed('day','trial')
    df = df.join(trial_start_dates, on = ['person_id','trial'], how = 'inner')

    # 4. Currently, date is time-invariant, and reflects the trial start date for the patient. Create a time-varying date column by summing date with time (minus 1)
    df = df.withColumn('trial_date', expr('date'))
    df = df.withColumn('time', expr('CAST(time AS int)')).withColumn('date', expr('DATE_ADD(date, time) - 1'))
    
    # # 5A. Get the outcome indicator and date and time value and join to the cross-join dataset. Limit to only rows up to the maximum follow-up length
    # outcome = propensity_model_prep_expanded.select(['person_id'] + outcome_columns).distinct()
    df = df.join(long_covid_df, on = 'person_id', how = 'left')
    df = df.where(expr('time <= {}'.format(follow_up_length)))

    # Create long COVID time column; take the the long covid date, subtract the trial date. The max event time is 180 days, plus the addiional 70 days to account for the end of the last window. The min time should be 28 days; 
    df = df.withColumn('long_covid_time', expr('DATEDIFF(long_covid_date, trial_date)')).withColumn('long_covid_time', expr('CASE WHEN long_covid_time IS NULL OR long_covid_time >= {} THEN {} ELSE long_covid_time END'.format(time_end, time_end)))
    # df = df.withColumn('long_covid_time', expr('CASE WHEN long_covid_time < 28 THEN 28 ELSE long_covid_time END'))

    # For patients who get long COVID, remove rows AFTER they got long COVID. For dates coming from the prediction model, we are currently using the END of the window as the long COVID date. However, if we want to use the START of the window, then set the condition below to TRUE. 
    use_start_of_window = False
    if use_start_of_window:
        df = df.withColumn('event_date', expr('CASE WHEN window_start IS NOT NULL THEN window_start ELSE long_covid_date END'))
    else:
        df = df.withColumn('event_date', expr('long_covid_date'))
    # Now filter to rows up through the day they get LC
    df = df.where(expr('(event_date IS NULL) OR (date <= event_date)')).drop('event_date')
    
    # Create time varying indicator for LC that turns 1 on the day of the event, and removes rows after it. 
    df = df.withColumn('outcome', expr('CASE WHEN date >= {} THEN 1 ELSE 0 END'.format(outcome_date))).withColumn('outcome', expr('SUM(outcome) OVER(PARTITION BY trial, person_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)')).where(expr('outcome <= 1'))

    # All rows occurring AFTER the death need to be removed. People should not die before getting Long COVID. 
    df = df.where(expr('(death_date IS NULL) OR (date <= death_date)'))

    # Aggregate by month. 
    df = df.withColumn('date_month', expr("DATE_TRUNC('MONTH', date)"))
    df = df.groupBy(['person_id','date_month']).agg( min(col('time')).alias('time'), min(col('trial')).alias('trial'), max(col(treatment_column)).alias(treatment_column), max(col('long_covid_date')).alias('long_covid_date'), max(col('long_covid')).alias('long_covid'), max(col('outcome')).alias('outcome'), max(col('long_covid_time')).alias('long_covid_time'), max(col('COVID_index_date')).alias('COVID_index_date'), max(col('subclass')).alias('subclass'))
    
    # Create accumulating month column
    df = df.withColumn('month', expr('ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY date_month)'))
    
    # Create time indicators, one per MONTH of follow-up; 
    for i in np.arange(1, months_max + 1):
        df = df.withColumn('d{}'.format(i), lit(0))
        df = df.withColumn('d{}'.format(i), expr('CASE WHEN month = {} THEN 1 ELSE d{} END'.format(i, i)))

    # Rename month to time; 
    df = df.drop('time').withColumnRenamed('month', 'time')

    # Create a row_id variable
    df = df.withColumn('row_id', expr('ROW_NUMBER() OVER(ORDER BY RAND())'))

    # Filter to the final columns we need for analysis
    time_indicator_features = ['d{}'.format(i) for i in np.arange(1, months_max+1)]
    main_columns = ['person_id','subclass','row_id','time','trial',treatment_column]
    target_column = ['outcome']
    final_columns = main_columns + target_column + time_indicator_features + outcome_columns
    
    # Filter the data frame to the final columns we need for analysis. 
    df = df.where(expr('time <= {}'.format(months_max))).select(final_columns)

    # To simplify the output dataframe, drop other columns
    df = df.drop("long_covid","long_covid_date","long_covid_time","trial")

    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c1562765-12f3-4715-baa4-c02852eabc73"),
    long_covid_date=Input(rid="ri.foundry.main.dataset.69dade5f-c43f-457a-89da-652a3e4a6e2a"),
    long_covid_date_v2=Input(rid="ri.foundry.main.dataset.5f0de766-9252-47b9-b981-c43c93a1181d"),
    nearest_neighbor_matching_LC=Input(rid="ri.foundry.main.dataset.6d3eb3b5-af0e-4831-9d10-c593c7158038"),
    paxlovid_expanded_data_long_covid=Input(rid="ri.foundry.main.dataset.3d2b0e13-8f89-43c2-b72a-f1e6cad12d67"),
    propensity_model_prep_expanded=Input(rid="ri.foundry.main.dataset.9999bcdb-ba34-4487-99b3-64a79b95e279")
)
def paxlovid_long_covid_coxreg_prep( long_covid_date, long_covid_date_v2, nearest_neighbor_matching_LC, propensity_model_prep_expanded, paxlovid_expanded_data_long_covid):

    # The purpose of this node is to expand the dataset for discrete time survival analysis; and to set up the dataset for Cox regression; 

    # import long covid table, change date to long_covid_date
    long_covid_df = long_covid_date_v2.withColumnRenamed('date','long_covid_date').withColumnRenamed('LONG_COVID','long_covid').select('person_id','long_covid_date','long_covid','window_start')

    # 1. Create a cross join between person id and days up to day 180; We add 70 to time end because - the final prediction window which occurs on month 6, ends 100 days after the start of the period.
    grace_period = 5
    treatment_column = 'treatment'
    time_end = 180 + 70
    follow_up_length = time_end
    outcome = 'long_covid'
    outcome_date = '{}_date'.format(outcome)
    outcome_columns = ['long_covid','long_covid_date','long_covid_time']
    
    # Create the cross join
    days = np.arange(1, time_end + 1)
    days_df = pd.DataFrame({'time': days})
    days_df = spark.createDataFrame(days_df)
    person_df = nearest_neighbor_matching_LC.select('person_id').distinct()
    full_join = person_df.join(days_df)
    
    # Join the cross join to matching output. In the matching output, each line is a person-trial. Each person-trial now gets joined to 180 days. 
    matching_output = nearest_neighbor_matching_LC.select('person_id','day','subclass','distance',treatment_column)
    df = matching_output.join(full_join, on = 'person_id', how = 'inner').withColumnRenamed('day','trial')

    # Get the trial start dates (date)
    trial_start_dates = propensity_model_prep_expanded.select('person_id','day','date','COVID_index_date','death_date').withColumnRenamed('day','trial')
    df = df.join(trial_start_dates, on = ['person_id','trial'], how = 'inner')

    # 4. Currently, date is time-invariant, and reflects the trial start date for the patient. Create a time-varying date column by summing date with time (minus 1)
    df = df.withColumn('trial_date', expr('date'))
    df = df.withColumn('time', expr('CAST(time AS int)')).withColumn('date', expr('DATE_ADD(date, time) - 1'))
    
    # # 5A. Get the outcome indicator and date and time value and join to the cross-join dataset. Limit to only rows up to the maximum follow-up length
    # outcome = propensity_model_prep_expanded.select(['person_id'] + outcome_columns).distinct()
    df = df.join(long_covid_df, on = 'person_id', how = 'left')
    df = df.where(expr('time <= {}'.format(follow_up_length)))

    # Create long COVID time column; take the the long covid date, subtract the trial date. The max event time is 180 days, plus the addiional 70 days to account for the end of the last window. The min time should be 28 days; 
    df = df.withColumn('long_covid_time', expr('DATEDIFF(long_covid_date, trial_date)')).withColumn('long_covid_time', expr('CASE WHEN long_covid_time IS NULL OR long_covid_time >= {} THEN {} ELSE long_covid_time END'.format(time_end, time_end)))
    # df = df.withColumn('long_covid_time', expr('CASE WHEN long_covid_time < 28 THEN 28 ELSE long_covid_time END'))

    # For patients who get long COVID, remove rows AFTER they got long COVID. For dates coming from the prediction model, we are currently using the END of the window as the long COVID date. However, if we want to use the START of the window, then set the condition below to TRUE. 
    use_start_of_window = True
    if use_start_of_window:
        df = df.withColumn('event_date', expr('CASE WHEN window_start IS NOT NULL THEN window_start ELSE long_covid_date END'))
    else:
        df = df.withColumn('event_date', expr('long_covid_date'))
    # Now filter to rows up through the day they get LC
    df = df.where(expr('(event_date IS NULL) OR (date <= event_date)')).drop('event_date')
    
    # Create time varying indicator for LC that turns 1 on the day of the event, and removes rows after it. 
    df = df.withColumn('outcome', expr('CASE WHEN date >= {} THEN 1 ELSE 0 END'.format(outcome_date))).withColumn('outcome', expr('SUM(outcome) OVER(PARTITION BY trial, person_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)')).where(expr('outcome <= 1'))

    # All rows occurring AFTER the death need to be removed. TPeople should not die before getting Long COVID. 
    df = df.where(expr('(death_date IS NULL) OR (date <= death_date)'))

    # Aggregate by month. 
    df = df.withColumn('date_month', expr("DATE_TRUNC('MONTH', date)"))
    df = df.groupBy(['person_id','date_month']).agg( min(col('time')).alias('time'), min(col('trial')).alias('trial'), max(col(treatment_column)).alias(treatment_column), max(col('long_covid_date')).alias('long_covid_date'), max(col('long_covid')).alias('long_covid'), max(col('outcome')).alias('outcome'), max(col('death_date')).alias('death_date'), max(col('long_covid_time')).alias('long_covid_time'), max(col('COVID_index_date')).alias('COVID_index_date'), max(col('subclass')).alias('subclass'))
    
    # Create accumulating month column
    df = df.withColumn('month', expr('ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY date_month)'))
    
    # Create time indicators, one per MONTH of follow-up; 
    months_max = 10
    for i in np.arange(1, months_max + 1):
        df = df.withColumn('d{}'.format(i), lit(0))
        df = df.withColumn('d{}'.format(i), expr('CASE WHEN month = {} THEN 1 ELSE d{} END'.format(i, i)))

    # Rename month to time; 
    df = df.drop('time').withColumnRenamed('month', 'time')

    # Create a row_id variable
    df = df.withColumn('row_id', expr('ROW_NUMBER() OVER(ORDER BY RAND())'))

    # Filter to the final columns we need for analysis
    time_indicator_features = ['d{}'.format(i) for i in np.arange(1, months_max+1)]
    main_columns = ['person_id','subclass','row_id','time','trial',treatment_column]
    target_column = ['outcome']
    final_columns = main_columns + target_column + time_indicator_features + outcome_columns
    
    # # 9. Filter the data frame to the final columns we need for analysis. 
    df = df.select(['person_id', 'subclass', treatment_column, 'long_covid_time','row_id'] + target_column).withColumn('outcome', expr('MAX(outcome) OVER(PARTITION BY person_id)')).where(expr('time = 1'))

    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a0cf0300-89fe-4b0b-8ce5-2a3701f600d3"),
    paxlovid_marginal_survival_curve_LC_bootstrap=Input(rid="ri.foundry.main.dataset.266f79f1-7315-4b3b-853a-58479ba33f03")
)
def paxlovid_marginal_survival_curve_LC(paxlovid_marginal_survival_curve_LC_bootstrap):

    treatment = 'treatment'
    time_end = 7

    # This node performs the bootstrapping survival curves and estimates the relative risk of the outcome on day 30

    df = paxlovid_marginal_survival_curve_LC_bootstrap.withColumn('treatment', expr('CASE WHEN {} = 0 THEN "control" ELSE "treatment" END'.format(treatment))).withColumnRenamed("time","timeline").select("timeline","treatment","bootstrap","survival")
    df = df.toPandas()
    df['bootstrap'] = df['bootstrap'].astype(int)
    df = df.set_index(['bootstrap','treatment','timeline'])
    df = df.unstack(level = 'treatment')
    print(df.shape)

    # Make sure the columns are treatment and control
    df.columns = df.columns.droplevel(level = 0)
    df = df.reset_index(drop = False)
    
    ##################################################
    # Now we can plot
    main_df = df
    
    # Right now we have 500 bootstrap survival curves
    # ("time","treatment","control","bootstrap")
    def lower_quantile(series):
        result = series.quantile(0.025)
        return result

    def upper_quantile(series):
        result = series.quantile(0.975)
        return result

    # We have to stack the data frames separately for treatment and control
    # df = main_df.where(col('bootstrap') != 999).toPandas()
    df = main_df.query('bootstrap != 999')
    df = df.set_index(['timeline','bootstrap'])
    df = df.rename_axis('treatment', axis=1)
    df = df.stack()
    df = pd.DataFrame(df)
    df.columns = ['surv']
    df['cum_inc'] = 1 - df['surv']
    df = df.reset_index(drop = False)
    print(df.head())

    ######## NEW CODE - REPEAT FOR THE OVERALL CURVE ##############
    df_overall = main_df.query('bootstrap == 999')
    df_overall = df_overall.set_index(['timeline','bootstrap'])
    df_overall = df_overall.rename_axis('treatment', axis=1)
    df_overall = df_overall.stack()
    df_overall = pd.DataFrame(df_overall)
    df_overall.columns = ['surv']
    df_overall['cum_inc'] = 1 - df_overall['surv']
    df_overall = df_overall.reset_index(drop = False)
    df_overall = df_overall.sort_values(by = ['treatment','timeline'])
    # print(df_overall)
    ###############################################################

    # Aggregate the curves by treatment and day; get the mean survival and the lower and upper limits
    df = df.groupby(['treatment', 'timeline']).agg(mean_cum_inc = ('cum_inc', np.mean),
    ll = ('cum_inc', lower_quantile),
    ul = ('cum_inc', upper_quantile)
    )
    
    df = df.reset_index()

    ### NOW WE CAN PLOT
    set_output_image_type('svg')
    fig, ax = plt.subplots(1,1, figsize = (11, 6))

    # # Plot the curves for each group
    # df_overall.query('treatment == "treatment"').plot(x = 'timeline', y = 'cum_inc', ax = ax, color = 'blue', drawstyle="steps-post") # Plot marginal survival curve (averaged) for treated group
    # df_overall.query('treatment == "control"').plot(x = 'timeline', y = 'cum_inc', ax = ax, color = 'orange', drawstyle="steps-post") # Plot the averaged marginal survival curve for the control group

    # Alternative plotting
    df_overall2 = df_overall.drop('surv', axis=1).set_index(['bootstrap','treatment','timeline']).unstack(level = 'treatment')
    df_overall2.index = df_overall2.index.droplevel(level = 0)
    df_overall2.columns = df_overall2.columns.droplevel(level = 0)
    df_overall2 = df_overall2.rename(columns = {'control':'Untreated', 'treatment':'Treated'})
    # df_overall2.plot(drawstyle="steps-post", ax = ax)
    df_overall2.plot(ax = ax)
    
    # ax.legend(['Treated', 'Untreated'])
    ax.legend(title = 'Group')

    # Plot the CI - first for the treated group (using fill_between)
    ax.fill_between(x = df.loc[df['treatment'] == "treatment", 'timeline'], 
                    y1 = df.loc[df['treatment'] == "treatment", 'll'], 
                    y2 = df.loc[df['treatment'] == "treatment", 'ul'], 
                    # color = 'orange', alpha = 0.2, step = 'post')
                    color = 'orange', alpha = 0.2)

    # PLot the CI for the control group
    ax.fill_between(x = df.loc[df['treatment'] == "control", 'timeline'], 
                    y1 = df.loc[df['treatment'] == "control", 'll'], 
                    y2 = df.loc[df['treatment'] == "control", 'ul'], 
                    # color = 'blue', alpha = 0.2, step = 'post')
                    color = 'blue', alpha = 0.2)

    # ax.set_ylim([0.0, 0.015])
    ax.set_title('Hospitalization', fontsize=11)
    ax.set_ylabel('Cumulative Incidence (%)', fontsize=10)
    ax.set_xlabel('Time (Days)', fontsize=10)
    # ax.set_xlim(0, 10)
    plt.show()

    # ##### NEXT WE WANT TO CALCULATE THE PROBABILITY DIFFERENCE ON DAY 7 AND THE RISK RATIO ON DAY 7
    # df = main_df.toPandas()
    df = main_df
    df = df.set_index(['timeline','bootstrap'])
    # Now calculate the probability difference
    df['treatment'] = 1 - df['treatment']
    df['control'] = 1 - df['control']
    df['treatment'] = df['treatment']
    df['control'] = df['control']
    df['risk_reduction'] = df['control'] - df['treatment']
    # Now take the risk ratio
    df['risk_ratio'] = df['treatment']/df['control']
    print(df.head())

    # Aggregate the curves by treatment and day; get the mean survival and the lower and upper limits
    df = df.query('bootstrap != 999') #### WE NEED TO ADD THIS
    df_statistics = df.groupby(['timeline']).agg(risk_reduction = ('risk_reduction', np.mean),
    risk_reduction_se = ('risk_reduction', np.std),
    risk_reduction_ll = ('risk_reduction', lower_quantile),
    risk_reduction_ul = ('risk_reduction', upper_quantile),
    # Get statistics for the risk ratio
    risk_ratio = ('risk_ratio', np.mean),
    risk_ratio_se = ('risk_ratio', np.std),
    risk_ratio_ll = ('risk_ratio', lower_quantile),
    risk_ratio_ul = ('risk_ratio', upper_quantile)
    )

    # Calculate the CI using the SE
    df_statistics['risk_ratio_lower95'] = df_statistics['risk_ratio'] - 1.96*df_statistics['risk_ratio_se']
    df_statistics['risk_ratio_upper95'] = df_statistics['risk_ratio'] + 1.96*df_statistics['risk_ratio_se']
    df_statistics = df_statistics.reset_index()

    ############################
    #### We need to swap the risk reduction and the risk ratio with the point estimate from the full sample
    control_cuminc = df_overall.loc[(df_overall['treatment'] == 'control') & (df_overall['timeline'] == time_end), 'cum_inc'].values[0]
    treatment_cuminc = df_overall.loc[(df_overall['treatment'] == 'treatment') & (df_overall['timeline'] == time_end), 'cum_inc'].values[0]
    risk_reduction = control_cuminc - treatment_cuminc
    risk_ratio = treatment_cuminc/control_cuminc

    # substitute those values into the table
    df_statistics.loc[(df_statistics['timeline'] == time_end), 'risk_ratio'] = risk_ratio
    df_statistics.loc[(df_statistics['timeline'] == time_end), 'risk_reduction'] = risk_reduction
    print(df_statistics.loc[df_statistics['timeline'] == time_end, ['risk_ratio','risk_ratio_ll','risk_ratio_ul']])
    ############################

    output_dataframe = df_statistics.loc[df_statistics['timeline'] == time_end, ['risk_ratio','risk_ratio_ll','risk_ratio_ul','risk_reduction','risk_reduction_ll','risk_reduction_ul']]
    output_dataframe.columns = ['Relative_Risk','Relative_Risk_Lower_Limit','Relative_Risk_Upper_Limit','Risk_Reduction','Risk_Reduction_Lower_Limit','Risk_Reduction_Upper_Limit']
    output_dataframe['drug'] = 'Paxlovid'

    return output_dataframe

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.266f79f1-7315-4b3b-853a-58479ba33f03"),
    paxlovid_expanded_data_long_covid=Input(rid="ri.foundry.main.dataset.3d2b0e13-8f89-43c2-b72a-f1e6cad12d67"),
    prediction_dataset_bootstrapping_paxlovid_LC=Input(rid="ri.foundry.main.dataset.b0bd1564-61f1-49d5-b7a5-64757695b687")
)
def paxlovid_marginal_survival_curve_LC_bootstrap(paxlovid_expanded_data_long_covid, prediction_dataset_bootstrapping_paxlovid_LC):

    import datetime
    import random
    from functools import reduce
    from pyspark.sql import DataFrame

    # This node will perform discrete time survival analysis to estimate the marginal survival curves for each treatment group; which are then used to estimate the relative risk of the outcome (hospitalization) on day 28

    # Steps:
    # 1. Take bootstrap
    # 2. Fit the propensity model and estimate the weights
    # 3. With the bootstrap - fit the DTSM
    # 4. Create prediction datasets 
    #   a. Prediction dataset for treated patients - use fully expanded dataset with ALL patients with treatment = 1
    #   b. Prediction dataset for control patients - use fully expanded dataset with ALL patients with treatment = 0
    # 5. Get predicted hazards in each dataset - under treated and under control
    # 6. Stack the datasets
    # 7. Average the hazards by day within each treatment condition = we will end up with 28 rows per treatment group
    # 8. Take the hazard complement (1 - H) and then calculate the survival curve as the cumulative product (make sure rows are sorted)
    # 9. We will end up with 28 rows per treatment group; with a "survival curve" column

    ################# NOW WE WILL FIT THE DTSA ###########################
    # Predictors are: treatment, day indicators, and the predictors
    df = paxlovid_expanded_data_long_covid
    # df = df.where(expr('trial <= 3'))

    time_end = 7
    time_indicator_features = ['d{}'.format(i) for i in np.arange(1, time_end+1)]
    treatment = 'treatment'
    outcome = 'outcome'
    id_column = 'person_id'
    time_column = 'time'
    matched_pair_column = 'subclass'
    
    # # Set up the outcome model features; and create our dataset for logistic regression (features + outcome)
    # predictors = ['treatment'] + time_indicator_features + features
    predictors = [treatment] + time_indicator_features
    # predictors_no_treatment = time_indicator_features + features
    predictors_no_treatment = time_indicator_features
    predictors_plus_outcome = predictors + [outcome]
    data_subset = df.select([matched_pair_column] + predictors_plus_outcome)

    ############# INCLUDE INTERACTIONS BETWEEN TREATMENT AND TIME INDICATORS ######
    # When we include this product - we do NOT include treatment as a predictor in the model ##### This will give us Tmt * Time, and Tmt * Predictors
    time_dependent_features = []
    for pred in predictors_no_treatment:
        data_subset = data_subset.withColumn('{treatment}_x_{predictor}'.format(treatment = treatment, predictor = pred), expr('{treatment} * {predictor}'.format(treatment = treatment, predictor = pred)))
        time_dependent_features.append('{treatment}_x_{predictor}'.format(treatment = treatment, predictor = pred))

    # # Calculate interactions between each time indicator AND each predictor. This will give us Time * Predictors
    # time_dependent_interactions = []
    # for time_indicator in time_indicator_features:
    #     for feature in features:
    #         data_subset = data_subset.withColumn('{}_x_{}'.format(time_indicator, feature), expr('{} * {}'.format(time_indicator, feature)))
    #         time_dependent_interactions.append('{}_x_{}'.format(time_indicator, feature))

    # Add the interactions between tmt * time indicators to the predictors object
    predictors = time_indicator_features + time_dependent_features
    print('final features:', predictors)

    ###### SET UP THE SAME FOR THE PREDICTION DATASET ###########
    # Set up the prediction df - this only contains the predictors. Calculate the interactions between tmt * time as above. 
    prediction_df = prediction_dataset_bootstrapping_paxlovid_LC
    for pred in predictors_no_treatment:
        prediction_df = prediction_df.withColumn('{treatment}_x_{predictor}'.format(treatment = treatment, predictor = pred), expr('{treatment} * {predictor}'.format(treatment = treatment, predictor = pred)))
        
    # for time_indicator in time_indicator_features:
    #     for feature in features:
    #         prediction_df = prediction_df.withColumn('{}_x_{}'.format(time_indicator, feature), expr('{} * {}'.format(time_indicator, feature)))
    
    ####### SPARK ML LOGISTIC REGRESSION MODEL #######################
    from pyspark.sql import SparkSession
    from pyspark import SparkFiles
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
    from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
    from pyspark.ml.feature import VectorAssembler

    ############ PERFORM BOOTSTRAPPING ##########################
    # Identify the unique subclasses - we will bootstrap the subclasses
    unique_subclasses = df.select('subclass').distinct()
    n_unique_subclasses = unique_subclasses.count()

    # Create empty list to store bootstrap output

    Output_Prediction_DataFrames = []
    
    # Now, perform the bootstrapping
    for i in np.arange(0, 300):

        # First - get a sample of the subclasses with replacement
        random.seed(a = i)
        sampled_subclasses_df = unique_subclasses.sample(fraction=1.0, seed=i, withReplacement=True)

        # Use the subclass dataframe to filter the original dataframe (randomly select matched pairs)
        data_subset_sample = sampled_subclasses_df.join(data_subset, on = 'subclass', how = 'inner')

        # Set up the vector assembler. The "predictors" object is a list containing the treatment, time indicators, and treatment * time indicators
        assembler = VectorAssembler(inputCols = predictors, outputCol = 'predictors')
        data_subset_sample = assembler.transform(data_subset_sample)
        prediction_subset = assembler.transform(prediction_df) # Transform the prediction_df also; 

        # Set up the LogisticRegression
        logistic_regression = LogisticRegression(featuresCol = 'predictors', 
        labelCol = outcome, 
        family = 'binomial', 
        maxIter = 1000, 
        regParam = 0.0, 
        elasticNetParam = 1.0,
        fitIntercept=False,
        # weightCol = 'SW'
        )

        # Fit the DTSA model to the data
        model = logistic_regression.fit(data_subset_sample)

        # # Print out the coefficients
        # coefficients = model.coefficients
        # intercept = model.intercept
        # print("Coefficients: ", coefficients)
        # print("Intercept: {:.3f}".format(intercept))

        # Get the predictions; Function below extracts the probability of the outcome (= 1)
        def ith_(v, i):
            try:
                return float(v[i])
            except ValueError:
                return None

        ith = udf(ith_, DoubleType())

        # Get predictions for the prediction data. We want the treatment column, day column, and the hazard probability 
        denominator_predictions = model.transform(prediction_subset).select([treatment, time_column] + [ith(col('probability'), lit(1)).alias('hazard')])
        denominator_predictions = denominator_predictions.withColumn('bootstrap', lit('{}'.format(i)))

        # A. Now get the survival function 
        denominator_predictions = denominator_predictions.withColumn('survival', expr('1 - hazard'))
        
        # Calculate the cumulative product of the hazard complements (the conditional survival probability) within each treatment. This is already grouped by treatment and time. 
        window = Window.partitionBy([treatment]).orderBy(time_column).rowsBetween(Window.unboundedPreceding,Window.currentRow)

        # Add the bootstrap column 
        denominator_predictions = denominator_predictions.withColumn('survival', product(col('survival')).over(window))

        # Append the prediction Spark dataframe to our list
        Output_Prediction_DataFrames.append(denominator_predictions)

        print('bootstrap {} completed'.format(i))

    ################ FIT THE MODEL IN THE FULL DATAT TO GET THE POINT ESTIMATE ##############################################

    #### SET UP ELEMENTS FOR SPARK LR
    assembler = VectorAssembler(inputCols = predictors, outputCol = 'predictors')

    # Transform the input dataset (optional) and the prediction dataset
    data_subset_sample = assembler.transform(data_subset)
    prediction_subset = assembler.transform(prediction_df)  

    # Set up the LogisticRegression; we are not using weights
    logistic_regression = LogisticRegression(featuresCol = 'predictors', 
    labelCol = outcome, 
    family = 'binomial', 
    maxIter = 1000, 
    regParam = 0.0, 
    elasticNetParam = 1.0,
    fitIntercept=False,
    # weightCol = weight_column,
    )

    # Fit the model to the data
    model = logistic_regression.fit(data_subset_sample)

    # # Print out the coefficients
    # coefficients = model.coefficients
    # intercept = model.intercept
    # print("Coefficients: ", coefficients)
    # print("Intercept: {:.3f}".format(intercept))

    # Get the predictions; Function below extracts the probability of the outcome (= 1)
    def ith_(v, i):
        try:
            return float(v[i])
        except ValueError:
            return None

    ith = udf(ith_, DoubleType())

    # Get predictions for the prediction data. We want the treatment column, day column, and the hazard probability 
    prediction_output_df = model.transform(prediction_subset).select([treatment, time_column] + [ith(col('probability'), lit(1)).alias('hazard')])
    prediction_output_df = prediction_output_df.withColumn('bootstrap', lit(999))

    # Get the survival function for each patient BEFORE averaging that
    prediction_output_df = prediction_output_df.withColumn('survival', expr('1 - hazard'))
    
    # Calculate cumulative product of conditional survival probability (within each treatment)
    window = Window.partitionBy([treatment]).orderBy(time_column).rowsBetween(Window.unboundedPreceding,Window.currentRow)
    prediction_output_df = prediction_output_df.withColumn('survival', product(col('survival')).over(window))

    # Append the point estimate dataframe to our list
    Output_Prediction_DataFrames.append(prediction_output_df)
    

    ##################### FINALLY CONCATENATE ALL THE PREDICTIONS OUTPUTS INTO A SINGLE DATA FRAME ####################
    denominator_predictions_stacked = reduce(DataFrame.unionAll, Output_Prediction_DataFrames)

    return denominator_predictions_stacked
    
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.15d987cb-52c0-4fae-a754-b2cc581c4a05"),
    paxlovid_bootstrap_DTSA_composite=Input(rid="ri.foundry.main.dataset.b8ad3326-707a-4073-9e8f-0c8317bdfb2e")
)
# Get Marginal Survival Curve AND Relative Risk (74510b8a-a5ee-4073-8a51-5eedf5f6d708): v3
def paxlovid_marginal_survival_curve_composite(paxlovid_bootstrap_DTSA_composite):
    DF = paxlovid_bootstrap_DTSA_composite

    treatment = 'treatment'
    time_end = 14
    step_plot = False

    # This node performs the bootstrapping survival curves and estimates the relative risk of the outcome on day 30

    df = DF.withColumn('treatment', expr('CASE WHEN {} = 0 THEN "control" ELSE "treatment" END'.format(treatment))).withColumnRenamed("time","timeline").select("timeline","treatment","bootstrap","survival")
    df = df.toPandas()
    df['bootstrap'] = df['bootstrap'].astype(int)
    df = df.set_index(['bootstrap','treatment','timeline'])
    df = df.unstack(level = 'treatment')
    print(df.shape)

    # Make sure the columns are treatment and control
    df.columns = df.columns.droplevel(level = 0)
    df = df.reset_index(drop = False)
    
    ##################################################
    # Now we can plot
    main_df = df
    
    # Right now we have 500 bootstrap survival curves
    # ("time","treatment","control","bootstrap")
    def lower_quantile(series):
        result = series.quantile(0.025)
        return result

    def upper_quantile(series):
        result = series.quantile(0.975)
        return result

    # We have to stack the data frames separately for treatment and control
    # df = main_df.where(col('bootstrap') != 999).toPandas()
    df = main_df.query('bootstrap != 999')
    df = df.set_index(['timeline','bootstrap'])
    df = df.rename_axis('treatment', axis=1)
    df = df.stack()
    df = pd.DataFrame(df)
    df.columns = ['surv']
    df['cum_inc'] = 1 - df['surv']
    df = df.reset_index(drop = False)
    print(df.head())

    ######## NEW CODE - REPEAT FOR THE OVERALL CURVE ##############
    df_overall = main_df.query('bootstrap == 999')
    df_overall = df_overall.set_index(['timeline','bootstrap'])
    df_overall = df_overall.rename_axis('treatment', axis=1)
    df_overall = df_overall.stack()
    df_overall = pd.DataFrame(df_overall)
    df_overall.columns = ['surv']
    df_overall['cum_inc'] = 1 - df_overall['surv']
    df_overall = df_overall.reset_index(drop = False)
    df_overall = df_overall.sort_values(by = ['treatment','timeline'])
    # print(df_overall)
    ###############################################################

    # Aggregate the curves by treatment and day; get the mean survival and the lower and upper limits
    df = df.groupby(['treatment', 'timeline']).agg(mean_cum_inc = ('cum_inc', np.mean),
    ll = ('cum_inc', lower_quantile),
    ul = ('cum_inc', upper_quantile)
    )
    
    df = df.reset_index()

    ### NOW WE CAN PLOT
    set_output_image_type('svg')
    fig, ax = plt.subplots(1,1, figsize = (11, 6))

    # # Plot the curves for each group
    # df_overall.query('treatment == "treatment"').plot(x = 'timeline', y = 'cum_inc', ax = ax, color = 'blue', drawstyle="steps-post") # Plot marginal survival curve (averaged) for treated group
    # df_overall.query('treatment == "control"').plot(x = 'timeline', y = 'cum_inc', ax = ax, color = 'orange', drawstyle="steps-post") # Plot the averaged marginal survival curve for the control group

    # Alternative plotting
    df_overall2 = df_overall.drop('surv', axis=1).set_index(['bootstrap','treatment','timeline']).unstack(level = 'treatment')
    df_overall2.index = df_overall2.index.droplevel(level = 0)
    df_overall2.columns = df_overall2.columns.droplevel(level = 0)
    df_overall2 = df_overall2.rename(columns = {'control':'Untreated', 'treatment':'Treated'})
    if step_plot:
        df_overall2.plot(drawstyle="steps-post", ax = ax)
    else:
        df_overall2.plot(ax = ax)
    
    # ax.legend(['Treated', 'Untreated'])
    ax.legend(title = 'Group')

    # Plot the CI - first for the treated group (using fill_between)
    if step_plot:
        ax.fill_between(x = df.loc[df['treatment'] == "treatment", 'timeline'], 
                        y1 = df.loc[df['treatment'] == "treatment", 'll'], 
                        y2 = df.loc[df['treatment'] == "treatment", 'ul'], 
                        color = 'orange', alpha = 0.2, step = 'post')

        # PLot the CI for the control group
        ax.fill_between(x = df.loc[df['treatment'] == "control", 'timeline'], 
                        y1 = df.loc[df['treatment'] == "control", 'll'], 
                        y2 = df.loc[df['treatment'] == "control", 'ul'], 
                        color = 'blue', alpha = 0.2, step = 'post')
    else:
        ax.fill_between(x = df.loc[df['treatment'] == "treatment", 'timeline'], 
                    y1 = df.loc[df['treatment'] == "treatment", 'll'], 
                    y2 = df.loc[df['treatment'] == "treatment", 'ul'], 
                    color = 'orange', alpha = 0.2)

        # PLot the CI for the control group
        ax.fill_between(x = df.loc[df['treatment'] == "control", 'timeline'], 
                        y1 = df.loc[df['treatment'] == "control", 'll'], 
                        y2 = df.loc[df['treatment'] == "control", 'ul'], 
                        color = 'blue', alpha = 0.2)

    ax.set_ylim([0.0, df['mean_cum_inc'].max() + 0.05 * df['mean_cum_inc'].max()])
    ax.set_title('Composite - ED visit, hospitalization, supplemental oxygen, or death', fontsize=11)
    ax.set_ylabel('Cumulative Incidence (%)', fontsize=10)
    ax.set_xlabel('Time (Days)', fontsize=10)
    plt.show()

    # ##### NEXT WE WANT TO CALCULATE THE PROBABILITY DIFFERENCE ON DAY 28 AND THE RISK RATIO ON DAY 28
    # df = main_df.toPandas()
    df = main_df
    df = df.set_index(['timeline','bootstrap'])
    # Now calculate the probability difference
    df['treatment'] = 1 - df['treatment']
    df['control'] = 1 - df['control']
    df['treatment'] = df['treatment']
    df['control'] = df['control']
    df['risk_reduction'] = df['control'] - df['treatment']
    # Now take the risk ratio
    df['risk_ratio'] = df['treatment']/df['control']
    print(df.head())

    # Aggregate the curves by treatment and day; get the mean survival and the lower and upper limits
    df = df.query('bootstrap != 999') #### WE NEED TO ADD THIS
    df_statistics = df.groupby(['timeline']).agg(risk_reduction = ('risk_reduction', np.mean),
    risk_reduction_se = ('risk_reduction', np.std),
    risk_reduction_ll = ('risk_reduction', lower_quantile),
    risk_reduction_ul = ('risk_reduction', upper_quantile),
    # Get statistics for the risk ratio
    risk_ratio = ('risk_ratio', np.mean),
    risk_ratio_se = ('risk_ratio', np.std),
    risk_ratio_ll = ('risk_ratio', lower_quantile),
    risk_ratio_ul = ('risk_ratio', upper_quantile)
    )

    # Calculate the CI using the SE
    df_statistics['risk_ratio_lower95'] = df_statistics['risk_ratio'] - 1.96*df_statistics['risk_ratio_se']
    df_statistics['risk_ratio_upper95'] = df_statistics['risk_ratio'] + 1.96*df_statistics['risk_ratio_se']
    df_statistics = df_statistics.reset_index()

    ############################
    #### We need to swap the risk reduction and the risk ratio with the point estimate from the full sample
    control_cuminc = df_overall.loc[(df_overall['treatment'] == 'control') & (df_overall['timeline'] == time_end), 'cum_inc'].values[0]
    treatment_cuminc = df_overall.loc[(df_overall['treatment'] == 'treatment') & (df_overall['timeline'] == time_end), 'cum_inc'].values[0]
    risk_reduction = control_cuminc - treatment_cuminc
    risk_ratio = treatment_cuminc/control_cuminc

    # substitute those values into the table
    df_statistics.loc[(df_statistics['timeline'] == time_end), 'risk_ratio'] = risk_ratio
    df_statistics.loc[(df_statistics['timeline'] == time_end), 'risk_reduction'] = risk_reduction
    print(df_statistics.loc[df_statistics['timeline'] == time_end, ['risk_ratio','risk_ratio_ll','risk_ratio_ul']])
    ############################

    output_dataframe = df_statistics.loc[df_statistics['timeline'] == 14, ['risk_ratio','risk_ratio_ll','risk_ratio_ul','risk_reduction','risk_reduction_ll','risk_reduction_ul']]
    output_dataframe.columns = ['Relative_Risk','Relative_Risk_Lower_Limit','Relative_Risk_Upper_Limit','Risk_Reduction','Risk_Reduction_Lower_Limit','Risk_Reduction_Upper_Limit']
    output_dataframe['drug'] = 'Paxlovid'

    return output_dataframe

#################################################
## Global imports and functions included below ##
#################################################

######## GLOBAL CODE
# We can add this code to the GLOBAL CODE on the RHS pane
# Import the data types we will be using to functions and schemas
from pyspark.sql.types import StructType, DateType, StructField, StringType, IntegerType, FloatType, DoubleType
from pyspark.sql import Row

# Import functions and tools to make functions, and interact with columns with dot functions or SQL functions
from pyspark.sql.functions import lower, upper, col, udf, monotonically_increasing_id, to_date, trim, ltrim, rtrim, avg
from pyspark.sql.functions import length, size, unix_timestamp, from_unixtime, broadcast, to_timestamp, split, when, rand, count, round, countDistinct, product

# Additional Functions
from pyspark.sql.functions import min, max, col, mean, lit, sum, when, regexp_replace, lower, upper, concat_ws, to_date, floor, months_between, datediff, date_add, current_date, least, greatest, last_day, last, expr

# Functions for window functions
from pyspark.sql import Window
from pyspark.sql.functions import row_number

### PYTHON Functions
import datetime as dt

## GLOBAL PY CODE
### Import all necessary packages
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re

# Pandas functions
idx = pd.IndexSlice

# Viewing related functions
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c491c8ca-8112-422e-b361-d46b411e524d"),
    paxlovid_bootstrap_DTSA_compositedeathhosp=Input(rid="ri.foundry.main.dataset.a81d7b76-0066-4fc5-a522-5fdaf39b5128")
)
# Get Marginal Survival Curve AND Relative Risk (74510b8a-a5ee-4073-8a51-5eedf5f6d708): v3
def paxlovid_marginal_survival_curve_compositedeathhosp(paxlovid_bootstrap_DTSA_compositedeathhosp):
    DF = paxlovid_bootstrap_DTSA_compositedeathhosp

    treatment = 'treatment'
    time_end = 14
    step_plot = False

    # This node performs the bootstrapping survival curves and estimates the relative risk of the outcome on day 30

    df = DF.withColumn('treatment', expr('CASE WHEN {} = 0 THEN "control" ELSE "treatment" END'.format(treatment))).withColumnRenamed("time","timeline").select("timeline","treatment","bootstrap","survival")
    df = df.toPandas()
    df['bootstrap'] = df['bootstrap'].astype(int)
    df = df.set_index(['bootstrap','treatment','timeline'])
    df = df.unstack(level = 'treatment')
    print(df.shape)

    # Make sure the columns are treatment and control
    df.columns = df.columns.droplevel(level = 0)
    df = df.reset_index(drop = False)
    
    ##################################################
    # Now we can plot
    main_df = df
    
    # Right now we have 500 bootstrap survival curves
    # ("time","treatment","control","bootstrap")
    def lower_quantile(series):
        result = series.quantile(0.025)
        return result

    def upper_quantile(series):
        result = series.quantile(0.975)
        return result

    # We have to stack the data frames separately for treatment and control
    # df = main_df.where(col('bootstrap') != 999).toPandas()
    df = main_df.query('bootstrap != 999')
    df = df.set_index(['timeline','bootstrap'])
    df = df.rename_axis('treatment', axis=1)
    df = df.stack()
    df = pd.DataFrame(df)
    df.columns = ['surv']
    df['cum_inc'] = 1 - df['surv']
    df = df.reset_index(drop = False)
    print(df.head())

    ######## NEW CODE - REPEAT FOR THE OVERALL CURVE ##############
    df_overall = main_df.query('bootstrap == 999')
    df_overall = df_overall.set_index(['timeline','bootstrap'])
    df_overall = df_overall.rename_axis('treatment', axis=1)
    df_overall = df_overall.stack()
    df_overall = pd.DataFrame(df_overall)
    df_overall.columns = ['surv']
    df_overall['cum_inc'] = 1 - df_overall['surv']
    df_overall = df_overall.reset_index(drop = False)
    df_overall = df_overall.sort_values(by = ['treatment','timeline'])
    # print(df_overall)
    ###############################################################

    # Aggregate the curves by treatment and day; get the mean survival and the lower and upper limits
    df = df.groupby(['treatment', 'timeline']).agg(mean_cum_inc = ('cum_inc', np.mean),
    ll = ('cum_inc', lower_quantile),
    ul = ('cum_inc', upper_quantile)
    )
    
    df = df.reset_index()

    ### NOW WE CAN PLOT
    set_output_image_type('svg')
    fig, ax = plt.subplots(1,1, figsize = (11, 6))

    # # Plot the curves for each group
    # df_overall.query('treatment == "treatment"').plot(x = 'timeline', y = 'cum_inc', ax = ax, color = 'blue', drawstyle="steps-post") # Plot marginal survival curve (averaged) for treated group
    # df_overall.query('treatment == "control"').plot(x = 'timeline', y = 'cum_inc', ax = ax, color = 'orange', drawstyle="steps-post") # Plot the averaged marginal survival curve for the control group

    # Alternative plotting
    df_overall2 = df_overall.drop('surv', axis=1).set_index(['bootstrap','treatment','timeline']).unstack(level = 'treatment')
    df_overall2.index = df_overall2.index.droplevel(level = 0)
    df_overall2.columns = df_overall2.columns.droplevel(level = 0)
    df_overall2 = df_overall2.rename(columns = {'control':'Untreated', 'treatment':'Treated'})
    if step_plot:
        df_overall2.plot(drawstyle="steps-post", ax = ax)
    else:
        df_overall2.plot(ax = ax)
    
    # ax.legend(['Treated', 'Untreated'])
    ax.legend(title = 'Group')

    # Plot the CI - first for the treated group (using fill_between)
    if step_plot:
        ax.fill_between(x = df.loc[df['treatment'] == "treatment", 'timeline'], 
                        y1 = df.loc[df['treatment'] == "treatment", 'll'], 
                        y2 = df.loc[df['treatment'] == "treatment", 'ul'], 
                        color = 'orange', alpha = 0.2, step = 'post')

        # PLot the CI for the control group
        ax.fill_between(x = df.loc[df['treatment'] == "control", 'timeline'], 
                        y1 = df.loc[df['treatment'] == "control", 'll'], 
                        y2 = df.loc[df['treatment'] == "control", 'ul'], 
                        color = 'blue', alpha = 0.2, step = 'post')
    else:
        ax.fill_between(x = df.loc[df['treatment'] == "treatment", 'timeline'], 
                    y1 = df.loc[df['treatment'] == "treatment", 'll'], 
                    y2 = df.loc[df['treatment'] == "treatment", 'ul'], 
                    color = 'orange', alpha = 0.2)

        # PLot the CI for the control group
        ax.fill_between(x = df.loc[df['treatment'] == "control", 'timeline'], 
                        y1 = df.loc[df['treatment'] == "control", 'll'], 
                        y2 = df.loc[df['treatment'] == "control", 'ul'], 
                        color = 'blue', alpha = 0.2)

    ax.set_ylim([0.0, df['mean_cum_inc'].max() + 0.05 * df['mean_cum_inc'].max()])
    ax.set_title('Hospitalization or Death', fontsize=11)
    ax.set_ylabel('Cumulative Incidence (%)', fontsize=10)
    ax.set_xlabel('Time (Days)', fontsize=10)
    plt.show()

    # ##### NEXT WE WANT TO CALCULATE THE PROBABILITY DIFFERENCE ON DAY 28 AND THE RISK RATIO ON DAY 28
    # df = main_df.toPandas()
    df = main_df
    df = df.set_index(['timeline','bootstrap'])
    # Now calculate the probability difference
    df['treatment'] = 1 - df['treatment']
    df['control'] = 1 - df['control']
    df['treatment'] = df['treatment']
    df['control'] = df['control']
    df['risk_reduction'] = df['control'] - df['treatment']
    # Now take the risk ratio
    df['risk_ratio'] = df['treatment']/df['control']
    print(df.head())

    # Aggregate the curves by treatment and day; get the mean survival and the lower and upper limits
    df = df.query('bootstrap != 999') #### WE NEED TO ADD THIS
    df_statistics = df.groupby(['timeline']).agg(risk_reduction = ('risk_reduction', np.mean),
    risk_reduction_se = ('risk_reduction', np.std),
    risk_reduction_ll = ('risk_reduction', lower_quantile),
    risk_reduction_ul = ('risk_reduction', upper_quantile),
    # Get statistics for the risk ratio
    risk_ratio = ('risk_ratio', np.mean),
    risk_ratio_se = ('risk_ratio', np.std),
    risk_ratio_ll = ('risk_ratio', lower_quantile),
    risk_ratio_ul = ('risk_ratio', upper_quantile)
    )

    # Calculate the CI using the SE
    df_statistics['risk_ratio_lower95'] = df_statistics['risk_ratio'] - 1.96*df_statistics['risk_ratio_se']
    df_statistics['risk_ratio_upper95'] = df_statistics['risk_ratio'] + 1.96*df_statistics['risk_ratio_se']
    df_statistics = df_statistics.reset_index()

    ############################
    #### We need to swap the risk reduction and the risk ratio with the point estimate from the full sample
    control_cuminc = df_overall.loc[(df_overall['treatment'] == 'control') & (df_overall['timeline'] == time_end), 'cum_inc'].values[0]
    treatment_cuminc = df_overall.loc[(df_overall['treatment'] == 'treatment') & (df_overall['timeline'] == time_end), 'cum_inc'].values[0]
    risk_reduction = control_cuminc - treatment_cuminc
    risk_ratio = treatment_cuminc/control_cuminc

    # substitute those values into the table
    df_statistics.loc[(df_statistics['timeline'] == time_end), 'risk_ratio'] = risk_ratio
    df_statistics.loc[(df_statistics['timeline'] == time_end), 'risk_reduction'] = risk_reduction
    print(df_statistics.loc[df_statistics['timeline'] == time_end, ['risk_ratio','risk_ratio_ll','risk_ratio_ul']])
    ############################

    output_dataframe = df_statistics.loc[df_statistics['timeline'] == 14, ['risk_ratio','risk_ratio_ll','risk_ratio_ul','risk_reduction','risk_reduction_ll','risk_reduction_ul']]
    output_dataframe.columns = ['Relative_Risk','Relative_Risk_Lower_Limit','Relative_Risk_Upper_Limit','Risk_Reduction','Risk_Reduction_Lower_Limit','Risk_Reduction_Upper_Limit']
    output_dataframe['drug'] = 'Paxlovid'

    return output_dataframe

#################################################
## Global imports and functions included below ##
#################################################

######## GLOBAL CODE
# We can add this code to the GLOBAL CODE on the RHS pane
# Import the data types we will be using to functions and schemas
from pyspark.sql.types import StructType, DateType, StructField, StringType, IntegerType, FloatType, DoubleType
from pyspark.sql import Row

# Import functions and tools to make functions, and interact with columns with dot functions or SQL functions
from pyspark.sql.functions import lower, upper, col, udf, monotonically_increasing_id, to_date, trim, ltrim, rtrim, avg
from pyspark.sql.functions import length, size, unix_timestamp, from_unixtime, broadcast, to_timestamp, split, when, rand, count, round, countDistinct, product

# Additional Functions
from pyspark.sql.functions import min, max, col, mean, lit, sum, when, regexp_replace, lower, upper, concat_ws, to_date, floor, months_between, datediff, date_add, current_date, least, greatest, last_day, last, expr

# Functions for window functions
from pyspark.sql import Window
from pyspark.sql.functions import row_number

### PYTHON Functions
import datetime as dt

## GLOBAL PY CODE
### Import all necessary packages
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re

# Pandas functions
idx = pd.IndexSlice

# Viewing related functions
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.5510c5c0-44e4-4f2a-a943-17e7769544d6"),
    paxlovid_bootstrap_DTSA_death=Input(rid="ri.foundry.main.dataset.3993bec5-34a6-47d9-b910-6698d1c77119")
)
# Get Marginal Survival Curve AND Relative Risk (74510b8a-a5ee-4073-8a51-5eedf5f6d708): v3
def paxlovid_marginal_survival_curve_death(paxlovid_bootstrap_DTSA_death):
    DF = paxlovid_bootstrap_DTSA_death

    treatment = 'treatment'
    time_end = 14
    step_plot = False

    # This node performs the bootstrapping survival curves and estimates the relative risk of the outcome on day 30

    df = DF.withColumn('treatment', expr('CASE WHEN {} = 0 THEN "control" ELSE "treatment" END'.format(treatment))).withColumnRenamed("time","timeline").select("timeline","treatment","bootstrap","survival")
    df = df.toPandas()
    df['bootstrap'] = df['bootstrap'].astype(int)
    df = df.set_index(['bootstrap','treatment','timeline'])
    df = df.unstack(level = 'treatment')
    print(df.shape)

    # Make sure the columns are treatment and control
    df.columns = df.columns.droplevel(level = 0)
    df = df.reset_index(drop = False)
    
    ##################################################
    # Now we can plot
    main_df = df
    
    # Right now we have 500 bootstrap survival curves
    # ("time","treatment","control","bootstrap")
    def lower_quantile(series):
        result = series.quantile(0.025)
        return result

    def upper_quantile(series):
        result = series.quantile(0.975)
        return result

    # We have to stack the data frames separately for treatment and control
    # df = main_df.where(col('bootstrap') != 999).toPandas()
    df = main_df.query('bootstrap != 999')
    df = df.set_index(['timeline','bootstrap'])
    df = df.rename_axis('treatment', axis=1)
    df = df.stack()
    df = pd.DataFrame(df)
    df.columns = ['surv']
    df['cum_inc'] = 1 - df['surv']
    df = df.reset_index(drop = False)
    print(df.head())

    ######## NEW CODE - REPEAT FOR THE OVERALL CURVE ##############
    df_overall = main_df.query('bootstrap == 999')
    df_overall = df_overall.set_index(['timeline','bootstrap'])
    df_overall = df_overall.rename_axis('treatment', axis=1)
    df_overall = df_overall.stack()
    df_overall = pd.DataFrame(df_overall)
    df_overall.columns = ['surv']
    df_overall['cum_inc'] = 1 - df_overall['surv']
    df_overall = df_overall.reset_index(drop = False)
    df_overall = df_overall.sort_values(by = ['treatment','timeline'])
    # print(df_overall)
    ###############################################################

    # Aggregate the curves by treatment and day; get the mean survival and the lower and upper limits
    df = df.groupby(['treatment', 'timeline']).agg(mean_cum_inc = ('cum_inc', np.mean),
    ll = ('cum_inc', lower_quantile),
    ul = ('cum_inc', upper_quantile)
    )
    
    df = df.reset_index()

    ### NOW WE CAN PLOT
    set_output_image_type('svg')
    fig, ax = plt.subplots(1,1, figsize = (11, 6))

    # # Plot the curves for each group
    # df_overall.query('treatment == "treatment"').plot(x = 'timeline', y = 'cum_inc', ax = ax, color = 'blue', drawstyle="steps-post") # Plot marginal survival curve (averaged) for treated group
    # df_overall.query('treatment == "control"').plot(x = 'timeline', y = 'cum_inc', ax = ax, color = 'orange', drawstyle="steps-post") # Plot the averaged marginal survival curve for the control group

    # Alternative plotting
    df_overall2 = df_overall.drop('surv', axis=1).set_index(['bootstrap','treatment','timeline']).unstack(level = 'treatment')
    df_overall2.index = df_overall2.index.droplevel(level = 0)
    df_overall2.columns = df_overall2.columns.droplevel(level = 0)
    df_overall2 = df_overall2.rename(columns = {'control':'Untreated', 'treatment':'Treated'})
    if step_plot:
        df_overall2.plot(drawstyle="steps-post", ax = ax)
    else:
        df_overall2.plot(ax = ax)
    
    # ax.legend(['Treated', 'Untreated'])
    ax.legend(title = 'Group')

    # Plot the CI - first for the treated group (using fill_between)
    if step_plot:
        ax.fill_between(x = df.loc[df['treatment'] == "treatment", 'timeline'], 
                        y1 = df.loc[df['treatment'] == "treatment", 'll'], 
                        y2 = df.loc[df['treatment'] == "treatment", 'ul'], 
                        color = 'orange', alpha = 0.2, step = 'post')

        # PLot the CI for the control group
        ax.fill_between(x = df.loc[df['treatment'] == "control", 'timeline'], 
                        y1 = df.loc[df['treatment'] == "control", 'll'], 
                        y2 = df.loc[df['treatment'] == "control", 'ul'], 
                        color = 'blue', alpha = 0.2, step = 'post')
    else:
        ax.fill_between(x = df.loc[df['treatment'] == "treatment", 'timeline'], 
                    y1 = df.loc[df['treatment'] == "treatment", 'll'], 
                    y2 = df.loc[df['treatment'] == "treatment", 'ul'], 
                    color = 'orange', alpha = 0.2)

        # PLot the CI for the control group
        ax.fill_between(x = df.loc[df['treatment'] == "control", 'timeline'], 
                        y1 = df.loc[df['treatment'] == "control", 'll'], 
                        y2 = df.loc[df['treatment'] == "control", 'ul'], 
                        color = 'blue', alpha = 0.2)

    ax.set_ylim([0.0, df['mean_cum_inc'].max() + 0.05 * df['mean_cum_inc'].max()])
    ax.set_title('Death', fontsize=11)
    ax.set_ylabel('Cumulative Incidence (%)', fontsize=10)
    ax.set_xlabel('Time (Days)', fontsize=10)
    plt.show()

    # ##### NEXT WE WANT TO CALCULATE THE PROBABILITY DIFFERENCE ON DAY 28 AND THE RISK RATIO ON DAY 28
    # df = main_df.toPandas()
    df = main_df
    df = df.set_index(['timeline','bootstrap'])
    # Now calculate the probability difference
    df['treatment'] = 1 - df['treatment']
    df['control'] = 1 - df['control']
    df['treatment'] = df['treatment']
    df['control'] = df['control']
    df['risk_reduction'] = df['control'] - df['treatment']
    # Now take the risk ratio
    df['risk_ratio'] = df['treatment']/df['control']
    print(df.head())

    # Aggregate the curves by treatment and day; get the mean survival and the lower and upper limits
    df = df.query('bootstrap != 999') #### WE NEED TO ADD THIS
    df_statistics = df.groupby(['timeline']).agg(risk_reduction = ('risk_reduction', np.mean),
    risk_reduction_se = ('risk_reduction', np.std),
    risk_reduction_ll = ('risk_reduction', lower_quantile),
    risk_reduction_ul = ('risk_reduction', upper_quantile),
    # Get statistics for the risk ratio
    risk_ratio = ('risk_ratio', np.mean),
    risk_ratio_se = ('risk_ratio', np.std),
    risk_ratio_ll = ('risk_ratio', lower_quantile),
    risk_ratio_ul = ('risk_ratio', upper_quantile)
    )

    # Calculate the CI using the SE
    df_statistics['risk_ratio_lower95'] = df_statistics['risk_ratio'] - 1.96*df_statistics['risk_ratio_se']
    df_statistics['risk_ratio_upper95'] = df_statistics['risk_ratio'] + 1.96*df_statistics['risk_ratio_se']
    df_statistics = df_statistics.reset_index()

    ############################
    #### We need to swap the risk reduction and the risk ratio with the point estimate from the full sample
    control_cuminc = df_overall.loc[(df_overall['treatment'] == 'control') & (df_overall['timeline'] == time_end), 'cum_inc'].values[0]
    treatment_cuminc = df_overall.loc[(df_overall['treatment'] == 'treatment') & (df_overall['timeline'] == time_end), 'cum_inc'].values[0]
    risk_reduction = control_cuminc - treatment_cuminc
    risk_ratio = treatment_cuminc/control_cuminc

    # substitute those values into the table
    df_statistics.loc[(df_statistics['timeline'] == time_end), 'risk_ratio'] = risk_ratio
    df_statistics.loc[(df_statistics['timeline'] == time_end), 'risk_reduction'] = risk_reduction
    print(df_statistics.loc[df_statistics['timeline'] == time_end, ['risk_ratio','risk_ratio_ll','risk_ratio_ul']])
    ############################

    output_dataframe = df_statistics.loc[df_statistics['timeline'] == 14, ['risk_ratio','risk_ratio_ll','risk_ratio_ul','risk_reduction','risk_reduction_ll','risk_reduction_ul']]
    output_dataframe.columns = ['Relative_Risk','Relative_Risk_Lower_Limit','Relative_Risk_Upper_Limit','Risk_Reduction','Risk_Reduction_Lower_Limit','Risk_Reduction_Upper_Limit']
    output_dataframe['drug'] = 'Paxlovid'

    return output_dataframe

#################################################
## Global imports and functions included below ##
#################################################

######## GLOBAL CODE
# We can add this code to the GLOBAL CODE on the RHS pane
# Import the data types we will be using to functions and schemas
from pyspark.sql.types import StructType, DateType, StructField, StringType, IntegerType, FloatType, DoubleType
from pyspark.sql import Row

# Import functions and tools to make functions, and interact with columns with dot functions or SQL functions
from pyspark.sql.functions import lower, upper, col, udf, monotonically_increasing_id, to_date, trim, ltrim, rtrim, avg
from pyspark.sql.functions import length, size, unix_timestamp, from_unixtime, broadcast, to_timestamp, split, when, rand, count, round, countDistinct, product

# Additional Functions
from pyspark.sql.functions import min, max, col, mean, lit, sum, when, regexp_replace, lower, upper, concat_ws, to_date, floor, months_between, datediff, date_add, current_date, least, greatest, last_day, last, expr

# Functions for window functions
from pyspark.sql import Window
from pyspark.sql.functions import row_number

### PYTHON Functions
import datetime as dt

## GLOBAL PY CODE
### Import all necessary packages
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re

# Pandas functions
idx = pd.IndexSlice

# Viewing related functions
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.eea6696b-8640-4f79-86e9-d53c9aa772a7"),
    paxlovid_bootstrap_DTSA_modcomposite=Input(rid="ri.foundry.main.dataset.79d27717-1ad0-4c1a-9590-cc4beda25648")
)
# Get Marginal Survival Curve AND Relative Risk (74510b8a-a5ee-4073-8a51-5eedf5f6d708): v3
def paxlovid_marginal_survival_curve_modcomposite(paxlovid_bootstrap_DTSA_modcomposite):
    DF = paxlovid_bootstrap_DTSA_modcomposite

    treatment = 'treatment'
    time_end = 14
    step_plot = False

    # This node performs the bootstrapping survival curves and estimates the relative risk of the outcome on day 30

    df = DF.withColumn('treatment', expr('CASE WHEN {} = 0 THEN "control" ELSE "treatment" END'.format(treatment))).withColumnRenamed("time","timeline").select("timeline","treatment","bootstrap","survival")
    df = df.toPandas()
    df['bootstrap'] = df['bootstrap'].astype(int)
    df = df.set_index(['bootstrap','treatment','timeline'])
    df = df.unstack(level = 'treatment')
    print(df.shape)

    # Make sure the columns are treatment and control
    df.columns = df.columns.droplevel(level = 0)
    df = df.reset_index(drop = False)
    
    ##################################################
    # Now we can plot
    main_df = df
    
    # Right now we have 500 bootstrap survival curves
    # ("time","treatment","control","bootstrap")
    def lower_quantile(series):
        result = series.quantile(0.025)
        return result

    def upper_quantile(series):
        result = series.quantile(0.975)
        return result

    # We have to stack the data frames separately for treatment and control
    # df = main_df.where(col('bootstrap') != 999).toPandas()
    df = main_df.query('bootstrap != 999')
    df = df.set_index(['timeline','bootstrap'])
    df = df.rename_axis('treatment', axis=1)
    df = df.stack()
    df = pd.DataFrame(df)
    df.columns = ['surv']
    df['cum_inc'] = 1 - df['surv']
    df = df.reset_index(drop = False)
    print(df.head())

    ######## NEW CODE - REPEAT FOR THE OVERALL CURVE ##############
    df_overall = main_df.query('bootstrap == 999')
    df_overall = df_overall.set_index(['timeline','bootstrap'])
    df_overall = df_overall.rename_axis('treatment', axis=1)
    df_overall = df_overall.stack()
    df_overall = pd.DataFrame(df_overall)
    df_overall.columns = ['surv']
    df_overall['cum_inc'] = 1 - df_overall['surv']
    df_overall = df_overall.reset_index(drop = False)
    df_overall = df_overall.sort_values(by = ['treatment','timeline'])
    # print(df_overall)
    ###############################################################

    # Aggregate the curves by treatment and day; get the mean survival and the lower and upper limits
    df = df.groupby(['treatment', 'timeline']).agg(mean_cum_inc = ('cum_inc', np.mean),
    ll = ('cum_inc', lower_quantile),
    ul = ('cum_inc', upper_quantile)
    )
    
    df = df.reset_index()

    ### NOW WE CAN PLOT
    set_output_image_type('svg')
    fig, ax = plt.subplots(1,1, figsize = (11, 6))

    # # Plot the curves for each group
    # df_overall.query('treatment == "treatment"').plot(x = 'timeline', y = 'cum_inc', ax = ax, color = 'blue', drawstyle="steps-post") # Plot marginal survival curve (averaged) for treated group
    # df_overall.query('treatment == "control"').plot(x = 'timeline', y = 'cum_inc', ax = ax, color = 'orange', drawstyle="steps-post") # Plot the averaged marginal survival curve for the control group

    # Alternative plotting
    df_overall2 = df_overall.drop('surv', axis=1).set_index(['bootstrap','treatment','timeline']).unstack(level = 'treatment')
    df_overall2.index = df_overall2.index.droplevel(level = 0)
    df_overall2.columns = df_overall2.columns.droplevel(level = 0)
    df_overall2 = df_overall2.rename(columns = {'control':'Untreated', 'treatment':'Treated'})
    if step_plot:
        df_overall2.plot(drawstyle="steps-post", ax = ax)
    else:
        df_overall2.plot(ax = ax)
    
    # ax.legend(['Treated', 'Untreated'])
    ax.legend(title = 'Group')

    # Plot the CI - first for the treated group (using fill_between)
    if step_plot:
        ax.fill_between(x = df.loc[df['treatment'] == "treatment", 'timeline'], 
                        y1 = df.loc[df['treatment'] == "treatment", 'll'], 
                        y2 = df.loc[df['treatment'] == "treatment", 'ul'], 
                        color = 'orange', alpha = 0.2, step = 'post')

        # PLot the CI for the control group
        ax.fill_between(x = df.loc[df['treatment'] == "control", 'timeline'], 
                        y1 = df.loc[df['treatment'] == "control", 'll'], 
                        y2 = df.loc[df['treatment'] == "control", 'ul'], 
                        color = 'blue', alpha = 0.2, step = 'post')
    else:
        ax.fill_between(x = df.loc[df['treatment'] == "treatment", 'timeline'], 
                    y1 = df.loc[df['treatment'] == "treatment", 'll'], 
                    y2 = df.loc[df['treatment'] == "treatment", 'ul'], 
                    color = 'orange', alpha = 0.2)

        # PLot the CI for the control group
        ax.fill_between(x = df.loc[df['treatment'] == "control", 'timeline'], 
                        y1 = df.loc[df['treatment'] == "control", 'll'], 
                        y2 = df.loc[df['treatment'] == "control", 'ul'], 
                        color = 'blue', alpha = 0.2)

    ax.set_ylim([0.0, df['mean_cum_inc'].max() + 0.05 * df['mean_cum_inc'].max()])
    ax.set_title('Composite - ED visit, hospitalization, or death', fontsize=11)
    ax.set_ylabel('Cumulative Incidence (%)', fontsize=10)
    ax.set_xlabel('Time (Days)', fontsize=10)
    plt.show()

    # ##### NEXT WE WANT TO CALCULATE THE PROBABILITY DIFFERENCE ON DAY 28 AND THE RISK RATIO ON DAY 28
    # df = main_df.toPandas()
    df = main_df
    df = df.set_index(['timeline','bootstrap'])
    # Now calculate the probability difference
    df['treatment'] = 1 - df['treatment']
    df['control'] = 1 - df['control']
    df['treatment'] = df['treatment']
    df['control'] = df['control']
    df['risk_reduction'] = df['control'] - df['treatment']
    # Now take the risk ratio
    df['risk_ratio'] = df['treatment']/df['control']
    print(df.head())

    # Aggregate the curves by treatment and day; get the mean survival and the lower and upper limits
    df = df.query('bootstrap != 999') #### WE NEED TO ADD THIS
    df_statistics = df.groupby(['timeline']).agg(risk_reduction = ('risk_reduction', np.mean),
    risk_reduction_se = ('risk_reduction', np.std),
    risk_reduction_ll = ('risk_reduction', lower_quantile),
    risk_reduction_ul = ('risk_reduction', upper_quantile),
    # Get statistics for the risk ratio
    risk_ratio = ('risk_ratio', np.mean),
    risk_ratio_se = ('risk_ratio', np.std),
    risk_ratio_ll = ('risk_ratio', lower_quantile),
    risk_ratio_ul = ('risk_ratio', upper_quantile)
    )

    # Calculate the CI using the SE
    df_statistics['risk_ratio_lower95'] = df_statistics['risk_ratio'] - 1.96*df_statistics['risk_ratio_se']
    df_statistics['risk_ratio_upper95'] = df_statistics['risk_ratio'] + 1.96*df_statistics['risk_ratio_se']
    df_statistics = df_statistics.reset_index()

    ############################
    #### We need to swap the risk reduction and the risk ratio with the point estimate from the full sample
    control_cuminc = df_overall.loc[(df_overall['treatment'] == 'control') & (df_overall['timeline'] == time_end), 'cum_inc'].values[0]
    treatment_cuminc = df_overall.loc[(df_overall['treatment'] == 'treatment') & (df_overall['timeline'] == time_end), 'cum_inc'].values[0]
    risk_reduction = control_cuminc - treatment_cuminc
    risk_ratio = treatment_cuminc/control_cuminc

    # substitute those values into the table
    df_statistics.loc[(df_statistics['timeline'] == time_end), 'risk_ratio'] = risk_ratio
    df_statistics.loc[(df_statistics['timeline'] == time_end), 'risk_reduction'] = risk_reduction
    print(df_statistics.loc[df_statistics['timeline'] == time_end, ['risk_ratio','risk_ratio_ll','risk_ratio_ul']])
    ############################

    output_dataframe = df_statistics.loc[df_statistics['timeline'] == 14, ['risk_ratio','risk_ratio_ll','risk_ratio_ul','risk_reduction','risk_reduction_ll','risk_reduction_ul']]
    output_dataframe.columns = ['Relative_Risk','Relative_Risk_Lower_Limit','Relative_Risk_Upper_Limit','Risk_Reduction','Risk_Reduction_Lower_Limit','Risk_Reduction_Upper_Limit']
    output_dataframe['drug'] = 'Paxlovid'

    return output_dataframe

#################################################
## Global imports and functions included below ##
#################################################

######## GLOBAL CODE
# We can add this code to the GLOBAL CODE on the RHS pane
# Import the data types we will be using to functions and schemas
from pyspark.sql.types import StructType, DateType, StructField, StringType, IntegerType, FloatType, DoubleType
from pyspark.sql import Row

# Import functions and tools to make functions, and interact with columns with dot functions or SQL functions
from pyspark.sql.functions import lower, upper, col, udf, monotonically_increasing_id, to_date, trim, ltrim, rtrim, avg
from pyspark.sql.functions import length, size, unix_timestamp, from_unixtime, broadcast, to_timestamp, split, when, rand, count, round, countDistinct, product

# Additional Functions
from pyspark.sql.functions import min, max, col, mean, lit, sum, when, regexp_replace, lower, upper, concat_ws, to_date, floor, months_between, datediff, date_add, current_date, least, greatest, last_day, last, expr

# Functions for window functions
from pyspark.sql import Window
from pyspark.sql.functions import row_number

### PYTHON Functions
import datetime as dt

## GLOBAL PY CODE
### Import all necessary packages
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re

# Pandas functions
idx = pd.IndexSlice

# Viewing related functions
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3c4d041c-a2d8-46b6-8382-8ccae890879b"),
    expanded_dataset_for_outcome_analysis_matching=Input(rid="ri.foundry.main.dataset.32ef28d9-6ad8-4b41-8a63-c6e4e37036d9"),
    nearest_neighbor_matching_all=Input(rid="ri.foundry.main.dataset.7ed3e283-f6c6-43f0-baf6-ee29ea81c982")
)
# Prepare Prediction Dataset (526217de-22d9-4b74-a4ec-bee49edb1a8f): v0
def prediction_dataset_bootstrapping_composite(expanded_dataset_for_outcome_analysis_matching, nearest_neighbor_matching_all):

    use_simple_prediction_dataset = True

    ##### STEP 1 - CREATE CROSS JOIN BETWEEN DAYS 1-28 AND DISTINCT PERSON_ID #######
    # We need to expand the dataset so each person has 1 row per day
    time_end = 14
    time_column = 'time'
    trial_column = 'trial'
    treatment = 'treatment'
    id_column = 'person_id'
    days = np.arange(1, time_end + 1)
    days_df = pd.DataFrame({time_column: days})
    days_df = spark.createDataFrame(days_df)
    person_df = expanded_dataset_for_outcome_analysis_matching.select('person_id').distinct()
    
    # Create cross join
    full_join = person_df.join(days_df)
    
    # Join cross join to our input dataset to expand it
    df = nearest_neighbor_matching_all.select('person_id','trial','subclass','distance',treatment)
    df = full_join.join(df, on = 'person_id', how = 'inner')

    # # Join to the propensity model dataset to get the features. This will merge ALL predictors on each trial (trials 1-5) with every 30-day block (each block having days 1-30). DAY is the trial column. 
    # df = df.join(paxlovid_propensity_model_dataset_prep, on = ['person_id','day'], how = 'inner')

    ######## STEP 2 - CREATE THE TIME INDICATORS -28 TIME INDICATORS ###################################
    for i in np.arange(1, time_end + 1):
        df = df.withColumn('d{}'.format(i), lit(0))
        df = df.withColumn('d{}'.format(i), expr('CASE WHEN time = {} THEN 1 ELSE d{} END'.format(i, i)))

    #### What columns do we need in the prediction dataset? Treatment, Time, Predictors
    time_indicator_features = ['d{}'.format(i) for i in np.arange(1, time_end+1)]
    # predictors = Model_predictors['predictors'].tolist()

    # # Create a final list of output features. We will include "day" so we can aggregate later
    final_columns = [trial_column] + [time_column] + [treatment] + time_indicator_features 
    
    # Filter to the final column list
    df = df.select(['person_id'] + final_columns)

    # Create separate datasets where everyone is treatment and control and then stack them
    treated = df.withColumn(treatment, lit(1))
    control = df.withColumn(treatment, lit(0))

    # Union the datasets
    df = treated.union(control)

    # Group the datasets by treatment group and indicator
    aggregations = [mean(col(column)).alias(column) for column in time_indicator_features]
    
    if use_simple_prediction_dataset:
        # df = df.groupBy([id_column, treatment, time_column]).agg(*aggregations).orderBy([treatment, time_column])
        df = df.groupBy([treatment, time_column]).agg(*aggregations).orderBy([treatment, time_column])

    return df
    

#################################################
## Global imports and functions included below ##
#################################################

######## GLOBAL CODE
# We can add this code to the GLOBAL CODE on the RHS pane
# Import the data types we will be using to functions and schemas
from pyspark.sql.types import StructType, DateType, StructField, StringType, IntegerType, FloatType, DoubleType
from pyspark.sql import Row

# Import functions and tools to make functions, and interact with columns with dot functions or SQL functions
from pyspark.sql.functions import lower, upper, col, udf, monotonically_increasing_id, to_date, trim, ltrim, rtrim, avg
from pyspark.sql.functions import length, size, unix_timestamp, from_unixtime, broadcast, to_timestamp, split, when, rand, count, round, countDistinct, product

# Additional Functions
from pyspark.sql.functions import min, max, col, mean, lit, sum, when, regexp_replace, lower, upper, concat_ws, to_date, floor, months_between, datediff, date_add, current_date, least, greatest, last_day, last, expr

# Functions for window functions
from pyspark.sql import Window
from pyspark.sql.functions import row_number

### PYTHON Functions
import datetime as dt

## GLOBAL PY CODE
### Import all necessary packages
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re

# Pandas functions
idx = pd.IndexSlice

# Viewing related functions
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.fe128665-5d79-4cbb-9fad-a776395fa0a6"),
    expanded_dataset_for_outcome_analysis_matching_compositedeathhosp=Input(rid="ri.foundry.main.dataset.1b4818d5-123b-4f59-a7b8-a341d2330a85"),
    nearest_neighbor_matching_all=Input(rid="ri.foundry.main.dataset.7ed3e283-f6c6-43f0-baf6-ee29ea81c982")
)
# Prepare Prediction Dataset (526217de-22d9-4b74-a4ec-bee49edb1a8f): v0
def prediction_dataset_bootstrapping_compositedeathhosp(expanded_dataset_for_outcome_analysis_matching_compositedeathhosp, nearest_neighbor_matching_all):

    use_simple_prediction_dataset = True

    ##### STEP 1 - CREATE CROSS JOIN BETWEEN DAYS 1-28 AND DISTINCT PERSON_ID #######
    # We need to expand the dataset so each person has 1 row per day
    time_end = 14
    time_column = 'time'
    trial_column = 'trial'
    treatment = 'treatment'
    id_column = 'person_id'
    days = np.arange(1, time_end + 1)
    days_df = pd.DataFrame({time_column: days})
    days_df = spark.createDataFrame(days_df)
    person_df = expanded_dataset_for_outcome_analysis_matching_compositedeathhosp.select('person_id').distinct()
    
    # Create cross join
    full_join = person_df.join(days_df)
    
    # Join cross join to our input dataset to expand it
    df = nearest_neighbor_matching_all.select('person_id','trial','subclass','distance',treatment)
    df = full_join.join(df, on = 'person_id', how = 'inner')

    # # Join to the propensity model dataset to get the features. This will merge ALL predictors on each trial (trials 1-5) with every 30-day block (each block having days 1-30). DAY is the trial column. 
    # df = df.join(paxlovid_propensity_model_dataset_prep, on = ['person_id','day'], how = 'inner')

    ######## STEP 2 - CREATE THE TIME INDICATORS -28 TIME INDICATORS ###################################
    for i in np.arange(1, time_end + 1):
        df = df.withColumn('d{}'.format(i), lit(0))
        df = df.withColumn('d{}'.format(i), expr('CASE WHEN time = {} THEN 1 ELSE d{} END'.format(i, i)))

    #### What columns do we need in the prediction dataset? Treatment, Time, Predictors
    time_indicator_features = ['d{}'.format(i) for i in np.arange(1, time_end+1)]
    # predictors = Model_predictors['predictors'].tolist()

    # # Create a final list of output features. We will include "day" so we can aggregate later
    final_columns = [trial_column] + [time_column] + [treatment] + time_indicator_features 
    
    # Filter to the final column list
    df = df.select(['person_id'] + final_columns)

    # Create separate datasets where everyone is treatment and control and then stack them
    treated = df.withColumn(treatment, lit(1))
    control = df.withColumn(treatment, lit(0))

    # Union the datasets
    df = treated.union(control)

    # Group the datasets by treatment group and indicator
    aggregations = [mean(col(column)).alias(column) for column in time_indicator_features]
    
    if use_simple_prediction_dataset:
        # df = df.groupBy([id_column, treatment, time_column]).agg(*aggregations).orderBy([treatment, time_column])
        df = df.groupBy([treatment, time_column]).agg(*aggregations).orderBy([treatment, time_column])

    return df
    

#################################################
## Global imports and functions included below ##
#################################################

######## GLOBAL CODE
# We can add this code to the GLOBAL CODE on the RHS pane
# Import the data types we will be using to functions and schemas
from pyspark.sql.types import StructType, DateType, StructField, StringType, IntegerType, FloatType, DoubleType
from pyspark.sql import Row

# Import functions and tools to make functions, and interact with columns with dot functions or SQL functions
from pyspark.sql.functions import lower, upper, col, udf, monotonically_increasing_id, to_date, trim, ltrim, rtrim, avg
from pyspark.sql.functions import length, size, unix_timestamp, from_unixtime, broadcast, to_timestamp, split, when, rand, count, round, countDistinct, product

# Additional Functions
from pyspark.sql.functions import min, max, col, mean, lit, sum, when, regexp_replace, lower, upper, concat_ws, to_date, floor, months_between, datediff, date_add, current_date, least, greatest, last_day, last, expr

# Functions for window functions
from pyspark.sql import Window
from pyspark.sql.functions import row_number

### PYTHON Functions
import datetime as dt

## GLOBAL PY CODE
### Import all necessary packages
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re

# Pandas functions
idx = pd.IndexSlice

# Viewing related functions
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.243ad79a-ef32-4cde-9400-9c8ae7b35c81"),
    expanded_dataset_for_outcome_analysis_matching_deathoutcome=Input(rid="ri.foundry.main.dataset.7fcd678b-62da-48b1-90e6-1c7e11ec5065"),
    nearest_neighbor_matching_all=Input(rid="ri.foundry.main.dataset.7ed3e283-f6c6-43f0-baf6-ee29ea81c982")
)
# Prepare Prediction Dataset (526217de-22d9-4b74-a4ec-bee49edb1a8f): v0
def prediction_dataset_bootstrapping_death(expanded_dataset_for_outcome_analysis_matching_deathoutcome, nearest_neighbor_matching_all):

    use_simple_prediction_dataset = True

    ##### STEP 1 - CREATE CROSS JOIN BETWEEN DAYS 1-28 AND DISTINCT PERSON_ID #######
    # We need to expand the dataset so each person has 1 row per day
    time_end = 14
    time_column = 'time'
    trial_column = 'trial'
    treatment = 'treatment'
    id_column = 'person_id'
    days = np.arange(1, time_end + 1)
    days_df = pd.DataFrame({time_column: days})
    days_df = spark.createDataFrame(days_df)
    person_df = expanded_dataset_for_outcome_analysis_matching_deathoutcome.select('person_id').distinct()
    
    # Create cross join
    full_join = person_df.join(days_df)
    
    # Join cross join to our input dataset to expand it
    df = nearest_neighbor_matching_all.select('person_id','trial','subclass','distance',treatment)
    df = full_join.join(df, on = 'person_id', how = 'inner')

    # # Join to the propensity model dataset to get the features. This will merge ALL predictors on each trial (trials 1-5) with every 30-day block (each block having days 1-30). DAY is the trial column. 
    # df = df.join(paxlovid_propensity_model_dataset_prep, on = ['person_id','day'], how = 'inner')

    ######## STEP 2 - CREATE THE TIME INDICATORS -28 TIME INDICATORS ###################################
    for i in np.arange(1, time_end + 1):
        df = df.withColumn('d{}'.format(i), lit(0))
        df = df.withColumn('d{}'.format(i), expr('CASE WHEN time = {} THEN 1 ELSE d{} END'.format(i, i)))

    #### What columns do we need in the prediction dataset? Treatment, Time, Predictors
    time_indicator_features = ['d{}'.format(i) for i in np.arange(1, time_end+1)]
    # predictors = Model_predictors['predictors'].tolist()

    # # Create a final list of output features. We will include "day" so we can aggregate later
    final_columns = [trial_column] + [time_column] + [treatment] + time_indicator_features 
    
    # Filter to the final column list
    df = df.select(['person_id'] + final_columns)

    # Create separate datasets where everyone is treatment and control and then stack them
    treated = df.withColumn(treatment, lit(1))
    control = df.withColumn(treatment, lit(0))

    # Union the datasets
    df = treated.union(control)

    # Group the datasets by treatment group and indicator
    aggregations = [mean(col(column)).alias(column) for column in time_indicator_features]
    
    if use_simple_prediction_dataset:
        # df = df.groupBy([id_column, treatment, time_column]).agg(*aggregations).orderBy([treatment, time_column])
        df = df.groupBy([treatment, time_column]).agg(*aggregations).orderBy([treatment, time_column])

    return df
    

#################################################
## Global imports and functions included below ##
#################################################

######## GLOBAL CODE
# We can add this code to the GLOBAL CODE on the RHS pane
# Import the data types we will be using to functions and schemas
from pyspark.sql.types import StructType, DateType, StructField, StringType, IntegerType, FloatType, DoubleType
from pyspark.sql import Row

# Import functions and tools to make functions, and interact with columns with dot functions or SQL functions
from pyspark.sql.functions import lower, upper, col, udf, monotonically_increasing_id, to_date, trim, ltrim, rtrim, avg
from pyspark.sql.functions import length, size, unix_timestamp, from_unixtime, broadcast, to_timestamp, split, when, rand, count, round, countDistinct, product

# Additional Functions
from pyspark.sql.functions import min, max, col, mean, lit, sum, when, regexp_replace, lower, upper, concat_ws, to_date, floor, months_between, datediff, date_add, current_date, least, greatest, last_day, last, expr

# Functions for window functions
from pyspark.sql import Window
from pyspark.sql.functions import row_number

### PYTHON Functions
import datetime as dt

## GLOBAL PY CODE
### Import all necessary packages
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re

# Pandas functions
idx = pd.IndexSlice

# Viewing related functions
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b9c72c59-ef49-4bfc-b5d1-f00ddc4a4be1"),
    expanded_dataset_for_outcome_analysis_matching_modcomposite=Input(rid="ri.foundry.main.dataset.1a14ee76-280e-46a0-8072-d3adea949e40"),
    nearest_neighbor_matching_all=Input(rid="ri.foundry.main.dataset.7ed3e283-f6c6-43f0-baf6-ee29ea81c982")
)
# Prepare Prediction Dataset (526217de-22d9-4b74-a4ec-bee49edb1a8f): v0
def prediction_dataset_bootstrapping_modcomposite(expanded_dataset_for_outcome_analysis_matching_modcomposite, nearest_neighbor_matching_all):

    use_simple_prediction_dataset = True

    ##### STEP 1 - CREATE CROSS JOIN BETWEEN DAYS 1-28 AND DISTINCT PERSON_ID #######
    # We need to expand the dataset so each person has 1 row per day
    time_end = 14
    time_column = 'time'
    trial_column = 'trial'
    treatment = 'treatment'
    id_column = 'person_id'
    days = np.arange(1, time_end + 1)
    days_df = pd.DataFrame({time_column: days})
    days_df = spark.createDataFrame(days_df)
    person_df = expanded_dataset_for_outcome_analysis_matching_modcomposite.select('person_id').distinct()
    
    # Create cross join
    full_join = person_df.join(days_df)
    
    # Join cross join to our input dataset to expand it
    df = nearest_neighbor_matching_all.select('person_id','trial','subclass','distance',treatment)
    df = full_join.join(df, on = 'person_id', how = 'inner')

    # # Join to the propensity model dataset to get the features. This will merge ALL predictors on each trial (trials 1-5) with every 30-day block (each block having days 1-30). DAY is the trial column. 
    # df = df.join(paxlovid_propensity_model_dataset_prep, on = ['person_id','day'], how = 'inner')

    ######## STEP 2 - CREATE THE TIME INDICATORS -28 TIME INDICATORS ###################################
    for i in np.arange(1, time_end + 1):
        df = df.withColumn('d{}'.format(i), lit(0))
        df = df.withColumn('d{}'.format(i), expr('CASE WHEN time = {} THEN 1 ELSE d{} END'.format(i, i)))

    #### What columns do we need in the prediction dataset? Treatment, Time, Predictors
    time_indicator_features = ['d{}'.format(i) for i in np.arange(1, time_end+1)]
    # predictors = Model_predictors['predictors'].tolist()

    # # Create a final list of output features. We will include "day" so we can aggregate later
    final_columns = [trial_column] + [time_column] + [treatment] + time_indicator_features 
    
    # Filter to the final column list
    df = df.select(['person_id'] + final_columns)

    # Create separate datasets where everyone is treatment and control and then stack them
    treated = df.withColumn(treatment, lit(1))
    control = df.withColumn(treatment, lit(0))

    # Union the datasets
    df = treated.union(control)

    # Group the datasets by treatment group and indicator
    aggregations = [mean(col(column)).alias(column) for column in time_indicator_features]
    
    if use_simple_prediction_dataset:
        # df = df.groupBy([id_column, treatment, time_column]).agg(*aggregations).orderBy([treatment, time_column])
        df = df.groupBy([treatment, time_column]).agg(*aggregations).orderBy([treatment, time_column])

    return df
    

#################################################
## Global imports and functions included below ##
#################################################

######## GLOBAL CODE
# We can add this code to the GLOBAL CODE on the RHS pane
# Import the data types we will be using to functions and schemas
from pyspark.sql.types import StructType, DateType, StructField, StringType, IntegerType, FloatType, DoubleType
from pyspark.sql import Row

# Import functions and tools to make functions, and interact with columns with dot functions or SQL functions
from pyspark.sql.functions import lower, upper, col, udf, monotonically_increasing_id, to_date, trim, ltrim, rtrim, avg
from pyspark.sql.functions import length, size, unix_timestamp, from_unixtime, broadcast, to_timestamp, split, when, rand, count, round, countDistinct, product

# Additional Functions
from pyspark.sql.functions import min, max, col, mean, lit, sum, when, regexp_replace, lower, upper, concat_ws, to_date, floor, months_between, datediff, date_add, current_date, least, greatest, last_day, last, expr

# Functions for window functions
from pyspark.sql import Window
from pyspark.sql.functions import row_number

### PYTHON Functions
import datetime as dt

## GLOBAL PY CODE
### Import all necessary packages
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re

# Pandas functions
idx = pd.IndexSlice

# Viewing related functions
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b0bd1564-61f1-49d5-b7a5-64757695b687"),
    expanded_dataset_for_outcome_analysis_matching=Input(rid="ri.foundry.main.dataset.32ef28d9-6ad8-4b41-8a63-c6e4e37036d9"),
    nearest_neighbor_matching_all=Input(rid="ri.foundry.main.dataset.7ed3e283-f6c6-43f0-baf6-ee29ea81c982"),
    paxlovid_expanded_data_long_covid=Input(rid="ri.foundry.main.dataset.3d2b0e13-8f89-43c2-b72a-f1e6cad12d67")
)
def prediction_dataset_bootstrapping_paxlovid_LC(expanded_dataset_for_outcome_analysis_matching, nearest_neighbor_matching_all, paxlovid_expanded_data_long_covid):

    use_simple_prediction_dataset = True

    ##### STEP 1 - CREATE CROSS JOIN BETWEEN DAYS 1-28 AND DISTINCT PERSON_ID #######
    # We need to expand the dataset so each person has 1 row per day
    time_end = 7
    time_column = 'time'
    trial_column = 'trial'
    treatment = 'treatment'
    id_column = 'person_id'
    days = np.arange(1, time_end + 1)
    days_df = pd.DataFrame({time_column: days})
    days_df = spark.createDataFrame(days_df)
    person_df = expanded_dataset_for_outcome_analysis_matching.select('person_id').distinct()
    
    # Create cross join
    full_join = person_df.join(days_df)
    
    # Join cross join to our input dataset to expand it
    df = nearest_neighbor_matching_all.select('person_id','trial','subclass','distance',treatment)
    df = full_join.join(df, on = 'person_id', how = 'inner')

    # # Join to the propensity model dataset to get the features. This will merge ALL predictors on each trial (trials 1-5) with every 30-day block (each block having days 1-30). DAY is the trial column. 
    # df = df.join(paxlovid_propensity_model_dataset_prep, on = ['person_id','day'], how = 'inner')

    ######## STEP 2 - CREATE THE TIME INDICATORS -28 TIME INDICATORS ###################################
    for i in np.arange(1, time_end + 1):
        df = df.withColumn('d{}'.format(i), lit(0))
        df = df.withColumn('d{}'.format(i), expr('CASE WHEN time = {} THEN 1 ELSE d{} END'.format(i, i)))

    #### What columns do we need in the prediction dataset? Treatment, Time, Predictors
    time_indicator_features = ['d{}'.format(i) for i in np.arange(1, time_end+1)]
    # predictors = Model_predictors['predictors'].tolist()

    # # Create a final list of output features. We will include "day" so we can aggregate later
    final_columns = [trial_column] + [time_column] + [treatment] + time_indicator_features 
    
    # Filter to the final column list
    df = df.select(['person_id'] + final_columns)

    # Create separate datasets where everyone is treatment and control and then stack them
    treated = df.withColumn(treatment, lit(1))
    control = df.withColumn(treatment, lit(0))

    # Union the datasets
    df = treated.union(control)

    # Group the datasets by treatment group and indicator
    aggregations = [mean(col(column)).alias(column) for column in time_indicator_features]
    
    if use_simple_prediction_dataset:
        # df = df.groupBy([id_column, treatment, time_column]).agg(*aggregations).orderBy([treatment, time_column])
        df = df.groupBy([treatment, time_column]).agg(*aggregations).orderBy([treatment, time_column])

    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1537bb59-51b7-4765-a5b9-e78e6514d8f5"),
    cohort_code=Input(rid="ri.foundry.main.dataset.b6450c6b-55d3-41e3-8208-14fa0780cd41"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    customize_concept_sets=Input(rid="ri.foundry.main.dataset.de3b19a8-bb5d-4752-a040-4e338b103af9"),
    observations_filtered=Input(rid="ri.foundry.main.dataset.6151c319-494e-4b79-b05e-9533024a6174"),
    procedure_occurrence=Input(rid="ri.foundry.main.dataset.f6f0b5e0-a105-403a-a98f-0ee1c78137dc")
)
def procedures_filtered(cohort_code, concept_set_members, procedure_occurrence, customize_concept_sets, observations_filtered):
  
    # Create a table of person_ids for our cohort
    persons = cohort_code.select('person_id')
    
    # Inner join the procedures table to our cohort    
    procedures_df = procedure_occurrence.select('person_id','procedure_date','procedure_concept_id').where(col('procedure_date').isNotNull()).withColumnRenamed('procedure_date','date').withColumnRenamed('procedure_concept_id','concept_id').join(persons,'person_id','inner')

    # Filter the fusion sheet to "procedures" - procedures we are interested in
    fusion_df = customize_concept_sets.where(col('domain').contains('procedure')).select('concept_set_name','indicator_prefix')
    
    # Filter the concept_set_members to the procedure concept sets we are interested in by joining to the fusion sheet
    concepts_df = concept_set_members.select('concept_set_name', 'is_most_recent_version', 'concept_id').where(col('is_most_recent_version')=='true').join(fusion_df, 'concept_set_name', 'inner').select('concept_id','indicator_prefix')
 
    # Filter the procedures table to procedure concept sets/concepts we are interested in 
    df = procedures_df.join(concepts_df, 'concept_id', 'inner')
    
    # Pivot the above table with patient/date on columns, each procedure on the columns - and placing a value of 1 in the cells (indicating procedure occurred for patient on that day)    
    df = df.groupby('person_id','date').pivot('indicator_prefix').agg(lit(1)).na.fill(0)

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6c7e9241-4b27-4cc1-bef7-635ff4f820af"),
    propensity_model_prep_expanded=Input(rid="ri.foundry.main.dataset.9999bcdb-ba34-4487-99b3-64a79b95e279")
)
def propensity_model_all_formatching(propensity_model_prep_expanded):
    
    ##################################### SPARK LOGISTIC CODE ####################################
    # Take the df dataset - this should have all rows up to and including the day of the treatment

    df = propensity_model_prep_expanded

    from pyspark.sql.types import StructType, DateType, StructField, StringType, IntegerType, FloatType, DoubleType
    from pyspark.sql import Row
    
    # Should we fit the intercept?
    fit_intercept = False # We do not fit the intercept if we are using time indicators

    # Set up variables for time-dependent interactions
    time_dependent_covariates = True

    # Limit the time indicators
    grace_period = 5

    # Set up variables
    id_variable = 'person_id'
    target_variable = 'treatment'
    treatment = 'treatment'
    time_variable = 'day'
    
    # include the data partner as indicators?
    include_dp = False

    # Include VARIANT (original column) for matching 
    include_variant_for_matching = True
    include_vaccination_for_matching = True
    if include_vaccination_for_matching:
        df = df.withColumnRenamed('vaccinated_LVCF','vaccination_count')

    # Set up predictors
    import re
    covariate_columns = [column for column in df.columns if (('covariate_' in column) | ('condition' in column) | ('procedure' in column) | ('vaccinated' in column) | ('risk_factor' in column)) & ('LVCF' in column) & ('exclusion' not in column)]
    demographics = [column for column in df.columns if ('race_ethnicity' in column) | ('age_at_covid' in column)] + ['female']

    # bmi_columns = [
    #     "UNDERWEIGHT_LVCF",
    #     "NORMAL_LVCF",
    #     "OVERWEIGHT_LVCF",
    #     "OBESE_LVCF",
    #     "OBESE_CLASS_3_LVCF",
    #     "MISSING_BMI_LVCF",
    # ]

    bmi_columns = []
    
    outcome_columns = [
        "death_date"
        ,"death"
        ,"death_time"
        ,"death60"
        ,"death_time60"
        ,"death28"
        ,"death_time28"
        ,"death14"
        ,"death_time14"
        ,"hospitalization_date"
        ,"hospitalization"
        ,"hospitalization_time"
        ,"hospitalization60"
        ,"hospitalization_time60"
        ,"hospitalization28"
        ,"hospitalization_time28"
        ,"hospitalization14"
        ,"hospitalization_time14"
        ,"EDvisit_date"
        ,"EDvisit"
        ,"EDvisit_time"
        ,"EDvisit60"
        ,"EDvisit_time60"
        ,"EDvisit28"
        ,"EDvisit_time28"
        ,"EDvisit14"
        ,"EDvisit_time14"
        ,"composite_date"
        ,"composite"
        ,"composite_time"
        ,"composite60"
        ,"composite_time60"
        ,"composite28"
        ,"composite_time28"
        ,"composite14"
        ,"composite_time14"
        ]

    # Create final list of predictors
    full_predictors = [column for column in covariate_columns + demographics + bmi_columns if column in df.columns]

    ############## SET UP PROPENSITY MODEL PREDICTORS AND CALCULATE TIME INDICATORS PLUS INTERACTIONS ######################################################
    # Create time indicators
    time_indicator_columns = []
    for i in np.arange(1, grace_period + 1):
        df = df.withColumn('d{}'.format(i), expr('CASE WHEN day = {day} THEN 1 ELSE 0 END'.format(day = i)))
        time_indicator_columns.append('d{}'.format(i))

    # Create interactions between each time indicator and each predictor
    time_dependent_interaction_columns = []
    for time_indicator in time_indicator_columns:
        for predictor in full_predictors:
            df = df.withColumn('{}_x_{}'.format(time_indicator, predictor), expr('{} * {}'.format(time_indicator, predictor)))
            time_dependent_interaction_columns.append('{}_x_{}'.format(time_indicator, predictor))

    # For data partners - we will only include interactions with linear time (model taking too long to fit)
    # Create indicators for the data partners
    data_partner_columns = []
    data_partner_interactions = []
    if include_dp:
        gps = df.select('data_partner_id').distinct().toPandas()
        gps = gps['data_partner_id'].tolist()
        
        for i in gps:
            df = df.withColumn('data_partner_id_{}'.format(i), expr('CASE WHEN data_partner_id = "{}" THEN 1 ELSE 0 END'.format(i)))
            data_partner_columns.append('data_partner_id_{}'.format(i))

        # Multiply each data partner indicator with day
        for column in data_partner_columns:
            df = df.withColumn(column + '_x_day', expr('{} * day'.format(column)))
            data_partner_interactions.append(column + '_x_day')
        
    # Now we have the columns we need for the analysis. The propensity model is a DTSA, which includes only time indicators AND interactions between time indicators and features
    analysis_columns = [id_variable] + ['data_partner_id'] + [target_variable] + [time_variable, 'trial', 'date'] + time_indicator_columns + time_dependent_interaction_columns + ['variant'] + ['vaccination_count']

    if include_dp:
        analysis_columns += data_partner_columns + data_partner_interactions

    # Filter our dataset to those columns
    analysis_columns = [column for column in analysis_columns if column in df.columns]
    df = df.select(analysis_columns)
    
    ####### FIT THE SPARK ML LOGISTIC REGRESSION MODEL #######################
    from pyspark.sql import SparkSession
    from pyspark import SparkFiles
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
    from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
    from pyspark.ml.feature import VectorAssembler
    from sklearn.linear_model import LogisticRegression as LogisticRegressionSK

    # predictors = time_indicator_columns + time_dependent_interaction_columns + data_partner_columns
    predictors = time_indicator_columns + time_dependent_interaction_columns
    print('predictors:', predictors)
    print('number of features:', len(predictors))

    # # Prepare the X and y dataset
    # X = df.select(predictors).toPandas()
    # y = df.select(target_variable).toPandas()

    # # Set up logistic regression object and fit model
    # try:
    #     lr = LogisticRegressionSK(fit_intercept = False, solver = 'newton-cg', penalty = 'none')
    #     lr.fit(X, y)
    # except:
    #     lr = LogisticRegressionSK(fit_intercept = False, solver = 'newton-cg', penalty = None)
    #     lr.fit(X, y)

    # # Get predictions
    # probabilities = lr.predict_proba(X)[:, 1]

    # # Add this is a column to our final dataset
    # output_predictions = df.select(id_variable, time_variable,'trial','date','data_partner_id', target_variable).toPandas()
    # output_predictions['propensity'] = probabilities
    # output_predictions['logit'] = np.log(output_predictions['propensity'] / (1 - output_predictions['propensity']))

    # Prepare the X and y dataset
    X = df.select(predictors)
    y = df.select(target_variable)

    # Set up VectorAssembler. Input the list of features, give the list of features a name (outputCol)
    assembler = VectorAssembler(inputCols = predictors, outputCol = 'predictors')

    # Transform our input dataset using the assembler. This will add a "predictor" column to the input dataset. We should also include the death_date column
    logistic_regression_data = assembler.transform(df)

    # Set up the LogisticRegression; specify the name of the outputCol that holds the list of the features (from the vectorAssembler) and the target variable
    logistic_regression = LogisticRegression(featuresCol = 'predictors', 
    labelCol = target_variable, 
    family = 'binomial', 
    maxIter = 1000, 
    regParam = 0.0, 
    elasticNetParam = 1.0,
    # weightCol = 'SW',
    fitIntercept = fit_intercept,
    )

    # Fit the model to the input data
    model = logistic_regression.fit(logistic_regression_data)

    # Print out the coefficients
    coefficients = model.coefficients
    intercept = model.intercept
    print("Coefficients: ", coefficients)
    # print("Intercept: {:.3f}".format(intercept))

    # Get the predictions; Function below extracts the probability of the outcome (= 1)
    def ith_(v, i):
        try:
            return float(v[i])
        except ValueError:
            return None

    ith = udf(ith_, DoubleType())

    # Get the predictions APPLIED TO THE PREDICTION DATASET. Add the row-number index. We will use this for merging ############# NEW CODE
    if include_variant_for_matching:
        output_predictions = model.transform(logistic_regression_data).select(ith(col('probability'), lit(1)).alias('propensity'), id_variable, time_variable, 'trial', 'date', 'data_partner_id', target_variable, 'variant', 'vaccination_count')
    else:
        output_predictions = model.transform(logistic_regression_data).select(ith(col('probability'), lit(1)).alias('propensity'), id_variable, time_variable, 'trial', 'date','data_partner_id', target_variable,'vaccination_count')

    # Calculate Logit
    output_predictions = output_predictions.withColumn('logit', expr('LOG(propensity / (1 - propensity))'))

    # Return output. Drop anyone missing logit; 
    return output_predictions.dropna(subset = ['logit'])

    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.2fa000bf-ed2e-4c82-a84f-e5da8d3ae5d2"),
    long_covid_sample=Input(rid="ri.foundry.main.dataset.350d80cb-4977-435a-a21c-894648e6daf1"),
    propensity_model_prep_expanded=Input(rid="ri.foundry.main.dataset.9999bcdb-ba34-4487-99b3-64a79b95e279")
)
def propensity_model_all_formatching_LC( propensity_model_prep_expanded, long_covid_sample):
    
    ##################################### SPARK LOGISTIC CODE ####################################
    # Take the df dataset - this should have all rows up to and including the day of the treatment

    # Limit the time indicators
    grace_period = 5

    # Set up df
    df = propensity_model_prep_expanded.join(long_covid_sample, on = ['person_id','day'], how = 'inner')

    from pyspark.sql.types import StructType, DateType, StructField, StringType, IntegerType, FloatType, DoubleType
    from pyspark.sql import Row
    
    # Should we fit the intercept?
    fit_intercept = False # We do not fit the intercept if we are using time indicators

    # Set up variables for time-dependent interactions
    time_dependent_covariates = True

    # Set up variables
    id_variable = 'person_id'
    target_variable = 'treatment'
    treatment = 'treatment'
    time_variable = 'day'
    
    # include the data partner as indicators?
    include_dp = True

    # include the original variant column for exact matching? If False, will only include them if they were dummified. 
    include_variant_for_matching = True
    include_vaccination_for_matching = True
    if include_vaccination_for_matching:
        df = df.withColumnRenamed('vaccinated_LVCF','vaccination_count')

    # Set up predictors
    import re
    covariate_columns = [column for column in df.columns if (('covariate_' in column) | ('condition' in column) | ('procedure' in column) | ('vaccinated' in column) | ('risk_factor' in column)) & ('LVCF' in column) & ('exclusion' not in column)]
    demographics = [column for column in df.columns if ('race_ethnicity' in column) | ('age_at_covid' in column)] + ['female']

    # bmi_columns = [
    #     "UNDERWEIGHT_LVCF",
    #     "NORMAL_LVCF",
    #     "OVERWEIGHT_LVCF",
    #     "OBESE_LVCF",
    #     "OBESE_CLASS_3_LVCF",
    #     "MISSING_BMI_LVCF",
    # ]

    bmi_columns = []
    
    outcome_columns = [
        "death_date"
        ,"death"
        ,"death_time"
        ,"death60"
        ,"death_time60"
        ,"death28"
        ,"death_time28"
        ,"death14"
        ,"death_time14"
        ,"hospitalization_date"
        ,"hospitalization"
        ,"hospitalization_time"
        ,"hospitalization60"
        ,"hospitalization_time60"
        ,"hospitalization28"
        ,"hospitalization_time28"
        ,"hospitalization14"
        ,"hospitalization_time14"
        ,"EDvisit_date"
        ,"EDvisit"
        ,"EDvisit_time"
        ,"EDvisit60"
        ,"EDvisit_time60"
        ,"EDvisit28"
        ,"EDvisit_time28"
        ,"EDvisit14"
        ,"EDvisit_time14"
        ,"composite_date"
        ,"composite"
        ,"composite_time"
        ,"composite60"
        ,"composite_time60"
        ,"composite28"
        ,"composite_time28"
        ,"composite14"
        ,"composite_time14"
        ]

    # Create final list of predictors
    full_predictors = [column for column in covariate_columns + demographics + bmi_columns if column in df.columns]

    ############## SET UP PROPENSITY MODEL PREDICTORS AND CALCULATE TIME INDICATORS PLUS INTERACTIONS ######################################################
    # Create time indicators
    time_indicator_columns = []
    for i in np.arange(1, grace_period + 1):
        df = df.withColumn('d{}'.format(i), expr('CASE WHEN day = {day} THEN 1 ELSE 0 END'.format(day = i)))
        time_indicator_columns.append('d{}'.format(i))

    # Create interactions between each time indicator and each predictor
    time_dependent_interaction_columns = []
    for time_indicator in time_indicator_columns:
        for predictor in full_predictors:
            df = df.withColumn('{}_x_{}'.format(time_indicator, predictor), expr('{} * {}'.format(time_indicator, predictor)))
            time_dependent_interaction_columns.append('{}_x_{}'.format(time_indicator, predictor))

    # For data partners - we will only include interactions with linear time (model taking too long to fit)
    # Create indicators for the data partners
    data_partner_columns = []
    data_partner_interactions = []
    if include_dp:
        gps = df.select('data_partner_id').distinct().toPandas()
        gps = gps['data_partner_id'].tolist()
        
        for i in gps:
            df = df.withColumn('data_partner_id_{}'.format(i), expr('CASE WHEN data_partner_id = "{}" THEN 1 ELSE 0 END'.format(i)))
            data_partner_columns.append('data_partner_id_{}'.format(i))

        # Multiply each data partner indicator with day
        for column in data_partner_columns:
            df = df.withColumn(column + '_x_day', expr('{} * day'.format(column)))
            data_partner_interactions.append(column + '_x_day')
        
    # Now we have the columns we need for the analysis. The propensity model is a DTSA, which includes only time indicators AND interactions between time indicators and features
    analysis_columns = [id_variable] + ['data_partner_id'] + [target_variable] + [time_variable, 'trial', 'date'] + time_indicator_columns + time_dependent_interaction_columns + ['variant'] + ['vaccination_count']

    if include_dp:
        analysis_columns += data_partner_columns + data_partner_interactions

    # Filter our dataset to those columns
    analysis_columns = [column for column in analysis_columns if column in df.columns]
    df = df.select(analysis_columns)
    
    ####### FIT THE SPARK ML LOGISTIC REGRESSION MODEL #######################
    from pyspark.sql import SparkSession
    from pyspark import SparkFiles
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
    from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
    from pyspark.ml.feature import VectorAssembler
    from sklearn.linear_model import LogisticRegression as LogisticRegressionSK

    # predictors = time_indicator_columns + time_dependent_interaction_columns + data_partner_columns
    predictors = time_indicator_columns + time_dependent_interaction_columns
    print('predictors:', predictors)
    print('number of features:', len(predictors))

    # # Prepare the X and y dataset
    # X = df.select(predictors).toPandas()
    # y = df.select(target_variable).toPandas()

    # # Set up logistic regression object and fit model
    # try:
    #     lr = LogisticRegressionSK(fit_intercept = False, solver = 'newton-cg', penalty = 'none')
    #     lr.fit(X, y)
    # except:
    #     lr = LogisticRegressionSK(fit_intercept = False, solver = 'newton-cg', penalty = None)
    #     lr.fit(X, y)

    # # Get predictions
    # probabilities = lr.predict_proba(X)[:, 1]

    # # Add this is a column to our final dataset
    # output_predictions = df.select(id_variable, time_variable,'trial','date','data_partner_id', target_variable).toPandas()
    # output_predictions['propensity'] = probabilities
    # output_predictions['logit'] = np.log(output_predictions['propensity'] / (1 - output_predictions['propensity']))

    # Prepare the X and y dataset
    X = df.select(predictors)
    y = df.select(target_variable)

    # Set up VectorAssembler. Input the list of features, give the list of features a name (outputCol)
    assembler = VectorAssembler(inputCols = predictors, outputCol = 'predictors')

    # Transform our input dataset using the assembler. This will add a "predictor" column to the input dataset. We should also include the death_date column
    logistic_regression_data = assembler.transform(df)

    # Set up the LogisticRegression; specify the name of the outputCol that holds the list of the features (from the vectorAssembler) and the target variable
    logistic_regression = LogisticRegression(featuresCol = 'predictors', 
    labelCol = target_variable, 
    family = 'binomial', 
    maxIter = 1000, 
    regParam = 0.0, 
    elasticNetParam = 1.0,
    # weightCol = 'SW',
    fitIntercept = fit_intercept,
    )

    # Fit the model to the input data
    model = logistic_regression.fit(logistic_regression_data)

    # Print out the coefficients
    coefficients = model.coefficients
    intercept = model.intercept
    print("Coefficients: ", coefficients)
    # print("Intercept: {:.3f}".format(intercept))

    # Get the predictions; Function below extracts the probability of the outcome (= 1)
    def ith_(v, i):
        try:
            return float(v[i])
        except ValueError:
            return None

    ith = udf(ith_, DoubleType())

    # Get the predictions APPLIED TO THE PREDICTION DATASET. Add the row-number index. We will use this for merging ############# NEW CODE
    if include_variant_for_matching:
        output_predictions = model.transform(logistic_regression_data).select(ith(col('probability'), lit(1)).alias('propensity'), id_variable, time_variable, 'trial', 'date', 'data_partner_id', target_variable, 'variant', 'vaccination_count')
    else:
        output_predictions = model.transform(logistic_regression_data).select(ith(col('probability'), lit(1)).alias('propensity'), id_variable, time_variable, 'trial', 'date','data_partner_id', target_variable,'vaccination_count')

    # Calculate Logit
    output_predictions = output_predictions.withColumn('logit', expr('LOG(propensity / (1 - propensity))'))

    # Return output. Drop anyone missing logit; 
    return output_predictions.dropna(subset = ['logit'])

    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.90d544d7-c7c1-4267-ab20-a5ec7f49e9b9"),
    propensity_model_all_formatching=Input(rid="ri.foundry.main.dataset.6c7e9241-4b27-4cc1-bef7-635ff4f820af")
)
def propensity_model_all_weights_formatching( propensity_model_all_formatching):
    
    # This node simply calculates weights from the matched sample. We don't need these weights
    propensity_model_all = propensity_model_all_formatching

    # This node will calculate the weights; 
    strata_number = 15
    grace_period = 5

    # Set up variables
    id_variable = 'person_id'
    target_variable = 'treatment'
    treatment = 'treatment'
    time_variable = 'day'

    output_predictions = propensity_model_all
    
    # Calculate the weights
    ############################################
    # 1. CALCULATE IPTW - FOR ATE OR ATT #######
    weight_type = 'ATT'

    # a. Calculate ATE weights
    # Modify propensity score for the controls to be 1-propensity
    output_predictions['propensity_correct'] = np.where(output_predictions[treatment] == 0, 1 - output_predictions['propensity'], output_predictions['propensity'])

    # Calculate the inverse weights
    output_predictions['IPTW'] = 1/output_predictions['propensity_correct']

    # Stabilize the weights by using the mean of the propensity score for the group
    output_predictions['stabilizer'] = output_predictions.groupby(treatment)['propensity_correct'].transform(np.mean)
    output_predictions['SW'] = output_predictions['stabilizer'] * output_predictions['IPTW']
      
    # b. Calculate ATT weights. The IPTW is 1 for the treated groups, and p / 1-p for the control group. This is weighting by the odds
    output_predictions['IPTW_ATT'] = np.where(output_predictions[treatment] == 1, 1, output_predictions['propensity'] / (1 - output_predictions['propensity']))

    # ##################################################################
    # # 2. PLOT THE DISTRIBUTION OF THE LOGIT SCORES IN EACH GROUP
    # fig, ax = plt.subplots(2, 1, figsize = (8,8), sharex = True)
    # output_predictions.query('{} == 1'.format(treatment))['logit'].plot(kind = 'hist', bins = 50, linewidth = 1, edgecolor='black', ax = ax[0], color = 'blue')
    # output_predictions.query('{} == 0'.format(treatment))['logit'].plot(kind = 'hist', bins = 50, linewidth = 1, edgecolor='black', ax = ax[1], color = 'orange')
    # ax[0].set_title('Treatment')
    # ax[1].set_title('Control')
    # plt.show()
    
    # 3. CALCULATE THE REGION OF COMMON SUPPORT
    ### Take the minimum of the maximum propensity score of the two groups
    max_logit_t = output_predictions.query('{} == 1'.format(treatment))['logit'].max()
    max_logit_c = output_predictions.query('{} == 0'.format(treatment))['logit'].max()
    min_of_max_logit = np.min([max_logit_t, max_logit_c])

    ### Take the maximum of the minimum propensity score of the two groups
    min_logit_t = output_predictions.query('{} == 1'.format(treatment))['logit'].min()
    min_logit_c = output_predictions.query('{} == 0'.format(treatment))['logit'].min()
    max_of_min_logit = np.max([min_logit_t, min_logit_c])

    # 4. EXCLUDE SS OUTSIDE OF REGION OF COMMON SUPPORT - MUST COME BEFORE THE MMWS IS COMPUTED ###############
    exclude_outside_region_common_support = False
    if exclude_outside_region_common_support == True:
        output_predictions['include'] = np.where((output_predictions['logit'] >= max_of_min_logit) & (output_predictions['logit'] <= min_of_max_logit), 1, 0)

        # Drop those not included
        output_predictions = output_predictions.query('include == 1')
    #################################################################################################################

    ###########################################################################
    # 5. Calculate the MMWS - this will come after excluding Ss outside common support
    # First step we need to stratify by the logit score
    output_predictions['strata'] = pd.qcut(output_predictions['logit'], q = strata_number, labels = False)
    output_predictions['strata'] = output_predictions['strata']+1
    
    ## STEP 1 - PROPORTION TREATED IN EACH GROUP
    if weight_type == 'ATE':
        output_predictions['treated_proportion'] = output_predictions.groupby(treatment)[treatment].transform('count') / output_predictions[treatment].count()

        # Now within each strata calculate the proportion treated in one's group
        output_predictions['treated_in_strata'] = output_predictions.groupby(['strata', treatment])[treatment].transform('count') / output_predictions.groupby(['strata'])['strata'].transform('count')

        # Calculate the MMWS
        output_predictions['MMWS'] = output_predictions['treated_proportion'] / output_predictions['treated_in_strata']
    
    else: # Caclulate ATT
        
        output_predictions['treated_proportion'] = output_predictions.groupby('strata')[treatment].transform(np.sum) / output_predictions.groupby('strata')[treatment].transform('count')
        output_predictions['control_proportion'] = (output_predictions.groupby('strata')[treatment].transform('count') - output_predictions.groupby('strata')[treatment].transform(np.sum)) / output_predictions.groupby('strata')[treatment].transform('count')
        output_predictions['MMWS'] = output_predictions['treated_proportion'] / output_predictions['control_proportion']
        output_predictions['MMWS'] = np.where(output_predictions[treatment] == 1, 1, output_predictions['MMWS'])

    ################################################################################
    
    # We only need to take 1 row per person - the day they received their treatment; 
    output_predictions = output_predictions.query('day == trial')

    return output_predictions.query('propensity > 0 & propensity < 1')

    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.41cf61a7-c055-49df-aa97-408c434c6a9f"),
    propensity_model_all_formatching_LC=Input(rid="ri.foundry.main.dataset.2fa000bf-ed2e-4c82-a84f-e5da8d3ae5d2")
)
def propensity_model_all_weights_formatching_LC( propensity_model_all_formatching_LC):
  
    propensity_model_all = propensity_model_all_formatching_LC

    # This node will calculate the weights; 
    strata_number = 7
    grace_period = 5

    # Set up variables
    id_variable = 'person_id'
    target_variable = 'treatment'
    treatment = 'treatment'
    time_variable = 'day'

    output_predictions = propensity_model_all
    
    # Calculate the weights
    ############################################
    # 1. CALCULATE IPTW - FOR ATE OR ATT #######
    weight_type = 'ATT'

    # a. Calculate ATE weights
    # Modify propensity score for the controls to be 1-propensity
    output_predictions['propensity_correct'] = np.where(output_predictions[treatment] == 0, 1 - output_predictions['propensity'], output_predictions['propensity'])

    # Calculate the inverse weights
    output_predictions['IPTW'] = 1/output_predictions['propensity_correct']

    # Stabilize the weights by using the mean of the propensity score for the group
    output_predictions['stabilizer'] = output_predictions.groupby(treatment)['propensity_correct'].transform(np.mean)
    output_predictions['SW'] = output_predictions['stabilizer'] * output_predictions['IPTW']
      
    # b. Calculate ATT weights. The IPTW is 1 for the treated groups, and p / 1-p for the control group. This is weighting by the odds
    output_predictions['IPTW_ATT'] = np.where(output_predictions[treatment] == 1, 1, output_predictions['propensity'] / (1 - output_predictions['propensity']))

    # ##################################################################
    # # 2. PLOT THE DISTRIBUTION OF THE LOGIT SCORES IN EACH GROUP
    # fig, ax = plt.subplots(2, 1, figsize = (8,8), sharex = True)
    # output_predictions.query('{} == 1'.format(treatment))['logit'].plot(kind = 'hist', bins = 50, linewidth = 1, edgecolor='black', ax = ax[0], color = 'blue')
    # output_predictions.query('{} == 0'.format(treatment))['logit'].plot(kind = 'hist', bins = 50, linewidth = 1, edgecolor='black', ax = ax[1], color = 'orange')
    # ax[0].set_title('Treatment')
    # ax[1].set_title('Control')
    # plt.show()
    
    # 3. CALCULATE THE REGION OF COMMON SUPPORT
    ### Take the minimum of the maximum propensity score of the two groups
    max_logit_t = output_predictions.query('{} == 1'.format(treatment))['logit'].max()
    max_logit_c = output_predictions.query('{} == 0'.format(treatment))['logit'].max()
    min_of_max_logit = np.min([max_logit_t, max_logit_c])

    ### Take the maximum of the minimum propensity score of the two groups
    min_logit_t = output_predictions.query('{} == 1'.format(treatment))['logit'].min()
    min_logit_c = output_predictions.query('{} == 0'.format(treatment))['logit'].min()
    max_of_min_logit = np.max([min_logit_t, min_logit_c])

    # 4. EXCLUDE SS OUTSIDE OF REGION OF COMMON SUPPORT - MUST COME BEFORE THE MMWS IS COMPUTED ###############
    exclude_outside_region_common_support = False
    if exclude_outside_region_common_support == True:
        output_predictions['include'] = np.where((output_predictions['logit'] >= max_of_min_logit) & (output_predictions['logit'] <= min_of_max_logit), 1, 0)

        # Drop those not included
        output_predictions = output_predictions.query('include == 1')
    #################################################################################################################

    ###########################################################################
    # 5. Calculate the MMWS - this will come after excluding Ss outside common support
    # First step we need to stratify by the logit score
    output_predictions['strata'] = pd.qcut(output_predictions['logit'], q = strata_number, labels = False)
    output_predictions['strata'] = output_predictions['strata']+1
    
    ## STEP 1 - PROPORTION TREATED IN EACH GROUP
    if weight_type == 'ATE':
        output_predictions['treated_proportion'] = output_predictions.groupby(treatment)[treatment].transform('count') / output_predictions[treatment].count()

        # Now within each strata calculate the proportion treated in one's group
        output_predictions['treated_in_strata'] = output_predictions.groupby(['strata', treatment])[treatment].transform('count') / output_predictions.groupby(['strata'])['strata'].transform('count')

        # Calculate the MMWS
        output_predictions['MMWS'] = output_predictions['treated_proportion'] / output_predictions['treated_in_strata']
    
    else: # Caclulate ATT
        
        output_predictions['treated_proportion'] = output_predictions.groupby('strata')[treatment].transform(np.sum) / output_predictions.groupby('strata')[treatment].transform('count')
        output_predictions['control_proportion'] = (output_predictions.groupby('strata')[treatment].transform('count') - output_predictions.groupby('strata')[treatment].transform(np.sum)) / output_predictions.groupby('strata')[treatment].transform('count')
        output_predictions['MMWS'] = output_predictions['treated_proportion'] / output_predictions['control_proportion']
        output_predictions['MMWS'] = np.where(output_predictions[treatment] == 1, 1, output_predictions['MMWS'])

    ################################################################################
    
    # We only need to take 1 row per person - the day they received their treatment; 
    output_predictions = output_predictions.query('day == trial')

    return output_predictions.query('propensity > 0 & propensity < 1')

    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a537ae63-0cc1-45c1-94a5-c7a11c7992fb"),
    eligible_sample_EHR_all=Input(rid="ri.foundry.main.dataset.bb3f1155-74ee-4013-b621-67e08eca5aeb")
)
def propensity_model_prep(eligible_sample_EHR_all):

    # This dataset should have all the time indicators, the predictors for the propensity model (covariates), the outcomes, the time to outcomes (14d), outcome date
    include_ED = True

    df = eligible_sample_EHR_all
    print(df.columns)

    # primary_treatment = 'paxlovid_treatment'
    treatment_exposure_columns = ["paxlovid_treatment"]

    essential_columns = [
        "person_id",
        "treatment",
        "data_partner_id",
        "data_extraction_date",
        "date",
        "start_day",
        "end_day",
        "day",
        "COVID_index_date",
        "date_of_birth",
        "variant",
    ]

    demographics = [
        'female',
        "race_ethnicity_nhpi",
        "race_ethnicity_white",
        "race_ethnicity_hispanic_latino",
        "race_ethnicity_aian",
        "race_ethnicity_black",
        "race_ethnicity_asian",
        
    ] + [column for column in df.columns if 'age_at_covid' in column]

    # bmi_columns = [
    #     "UNDERWEIGHT_LVCF",
    #     "NORMAL_LVCF",
    #     "OVERWEIGHT_LVCF",
    #     "OBESE_LVCF",
    #     "OBESE_CLASS_3_LVCF",
    #     "MISSING_BMI_LVCF",
    # ]

    bmi_columns = []

    outcome_columns = [
        "death_date"
        ,"death"
        ,"death_time"
        ,"death60"
        ,"death_time60"
        ,"death28"
        ,"death_time28"
        ,"death14"
        ,"death_time14"
        ,"hospitalization_date"
        ,"hospitalization"
        ,"hospitalization_time"
        ,"hospitalization60"
        ,"hospitalization_time60"
        ,"hospitalization28"
        ,"hospitalization_time28"
        ,"hospitalization14"
        ,"hospitalization_time14"
        ,"EDvisit_date"
        ,"EDvisit"
        ,"EDvisit_time"
        ,"EDvisit60"
        ,"EDvisit_time60"
        ,"EDvisit28"
        ,"EDvisit_time28"
        ,"EDvisit14"
        ,"EDvisit_time14"
        ,"composite_date"
        ,"composite"
        ,"composite_time"
        ,"composite60"
        ,"composite_time60"
        ,"composite28"
        ,"composite_time28"
        ,"composite14"
        ,"composite_time14"
    ]

    covariate_columns = [column for column in df.columns if (('covariate_' in column) | ('condition' in column) | ('vaccinated' in column) | ('procedure' in column) | ('risk_factor' in column)) & ('exclusion' not in column) & ('LVCF' in column)]
    print('covariates', covariate_columns)

    all_columns = essential_columns + outcome_columns + demographics + bmi_columns + treatment_exposure_columns + covariate_columns
    all_columns = [column for column in all_columns if column in df.columns]
    print('all Predictors', all_columns)

    df = df.select(all_columns)

    return df

    

    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.9999bcdb-ba34-4487-99b3-64a79b95e279"),
    long_dataset_all=Input(rid="ri.foundry.main.dataset.fd0ccf46-d321-41d5-a119-2e57940a931d"),
    propensity_model_prep=Input(rid="ri.foundry.main.dataset.a537ae63-0cc1-45c1-94a5-c7a11c7992fb")
)
def propensity_model_prep_expanded(propensity_model_prep, long_dataset_all):
    
    # We need to expand the dataset 
    # Keep all rows until 24 hours before the date of mod_composite outcome

    # First - take all the rows for the grace period
    person_days = long_dataset_all.select('person_id','day')
    subset = 'all'

    # Join the input data to all rows of the grace period. Rename "day" to "trial" in the input data. Keep all rows up to an including the day of the trial (when the person got treated with either metformin or other treatment; this is technically a competing risk model).
    # df = person_days.join(propensity_model_prep_all_withlab.withColumnRenamed('day','trial'), on = ['person_id','day'], how = 'inner').where(expr('day <= trial'))
    df = person_days.join(propensity_model_prep, on = ['person_id','day'], how = 'inner')

    ############################ Make another composite outcome - excludes supp oxygen ######################################
    outcome_date_columns = [
        "death_date",
        "hospitalization_date",
        'EDvisit_date'
    ]
    
    # Create mod_composite column
    df = df.withColumn('mod_composite_date', expr('LEAST({})'.format(','.join(outcome_date_columns))))

    # mod_composite
    df = df.withColumn('mod_composite', expr('CASE WHEN mod_composite_date IS NOT NULL THEN 1 ELSE 0 END'))
    df = df.withColumn('mod_composite_time', expr('CASE WHEN mod_composite = 1 THEN DATEDIFF(mod_composite_date, date) + 1 ELSE DATEDIFF(end_day, date) + 1 END'))

    df = df.withColumn('mod_composite60', expr('CASE WHEN mod_composite_date IS NOT NULL AND mod_composite_time <= 60 THEN 1 ELSE 0 END'))
    df = df.withColumn('mod_composite_time60', expr('CASE WHEN mod_composite_time <= 60 THEN mod_composite_time ELSE 60 END'))

    df = df.withColumn('mod_composite28', expr('CASE WHEN mod_composite_date IS NOT NULL AND mod_composite_time <= 28 THEN 1 ELSE 0 END'))
    df = df.withColumn('mod_composite_time28', expr('CASE WHEN mod_composite_time <= 28 THEN mod_composite_time ELSE 28 END'))

    df = df.withColumn('mod_composite14', expr('CASE WHEN mod_composite_date IS NOT NULL AND mod_composite_time <= 14 THEN 1 ELSE 0 END'))
    df = df.withColumn('mod_composite_time14', expr('CASE WHEN mod_composite_time <= 14 THEN mod_composite_time ELSE 14 END'))

    #########################

    outcome_date_columns = [
        "death_date",
        "hospitalization_date",
    ]
    
    # Create composite_death_hosp column
    df = df.withColumn('composite_death_hosp_date', expr('LEAST({})'.format(','.join(outcome_date_columns))))

    # composite_death_hosp
    df = df.withColumn('composite_death_hosp', expr('CASE WHEN composite_death_hosp_date IS NOT NULL THEN 1 ELSE 0 END'))
    df = df.withColumn('composite_death_hosp_time', expr('CASE WHEN composite_death_hosp = 1 THEN DATEDIFF(composite_death_hosp_date, date) + 1 ELSE DATEDIFF(end_day, date) + 1 END'))

    df = df.withColumn('composite_death_hosp60', expr('CASE WHEN composite_death_hosp_date IS NOT NULL AND composite_death_hosp_time <= 60 THEN 1 ELSE 0 END'))
    df = df.withColumn('composite_death_hosp_time60', expr('CASE WHEN composite_death_hosp_time <= 60 THEN composite_death_hosp_time ELSE 60 END'))

    df = df.withColumn('composite_death_hosp28', expr('CASE WHEN composite_death_hosp_date IS NOT NULL AND composite_death_hosp_time <= 28 THEN 1 ELSE 0 END'))
    df = df.withColumn('composite_death_hosp_time28', expr('CASE WHEN composite_death_hosp_time <= 28 THEN composite_death_hosp_time ELSE 28 END'))

    df = df.withColumn('composite_death_hosp14', expr('CASE WHEN composite_death_hosp_date IS NOT NULL AND composite_death_hosp_time <= 14 THEN 1 ELSE 0 END'))
    df = df.withColumn('composite_death_hosp_time14', expr('CASE WHEN composite_death_hosp_time <= 14 THEN composite_death_hosp_time ELSE 14 END'))
    
    #######################################
    df = df.withColumn('trial', col('day'))

    ################### APPLY FILTERS FOR SAMPLE ##################### 
    # Filters include - vaccinated, unvaccinated, early treatment, late treatment
    subset_samples = ['vax','unvax','early','late','all']
    
    if subset == 'vax':
        df = df.where(col('vaccinated_LVCF') > 0)
    elif subset == 'unvax':
        df = df.where(col('vaccinated_LVCF') == 0)
    elif subset == 'early':
        df = df.where(col('day') <= 2)
    elif subset == 'late':
        df = df.where(col('day') > 2)

    return df.orderBy('person_id','day')
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3ce9c84e-3810-4a14-93c3-b73000249aa8"),
    DESCRIPTIVES_PRE_MATCHING=Input(rid="ri.foundry.main.dataset.cfe6929b-fb38-4400-b787-0ced00b2a65b"),
    eligible_sample_EHR_all=Input(rid="ri.foundry.main.dataset.bb3f1155-74ee-4013-b621-67e08eca5aeb"),
    nearest_neighbor_matching_all=Input(rid="ri.foundry.main.dataset.7ed3e283-f6c6-43f0-baf6-ee29ea81c982")
)
def testing_balance_copied_1(nearest_neighbor_matching_all, eligible_sample_EHR_all, DESCRIPTIVES_PRE_MATCHING):
    DESCRIPTIVES_PRE_MATCHING_copied = DESCRIPTIVES_PRE_MATCHING
    
    # What is the treatment column?
    treatment = 'treatment '

    ### First set up the matching input dataset and matching output dataset
    match_input = eligible_sample_EHR_all
    match_output = nearest_neighbor_matching_all.select('person_id','day','subclass','logit')

    # Set up column lists: demographics, BMI, vaccination, covariates (drugs and procedures), and optionally - other treatments
    treatment_exposure_columns = ["montelukast_treatment","fluvoxamine_treatment","ivermectin_treatment"]

    demographics = [
        'female',
        "race_ethnicity_nhpi",
        "race_ethnicity_white",
        "race_ethnicity_hispanic_latino",
        "race_ethnicity_aian",
        "race_ethnicity_black",
        "race_ethnicity_asian",
        # 'age_at_covid',
    ] + [column for column in match_input.columns if 'age_at_covid' in column]

    # bmi_columns = [
    #     "UNDERWEIGHT_LVCF",
    #     "NORMAL_LVCF",
    #     "OVERWEIGHT_LVCF",
    #     "OBESE_LVCF",
    #     "OBESE_CLASS_3_LVCF",
    #     "MISSING_BMI_LVCF",
    # ]

    bmi_columns = []

    covariate_columns = [column for column in match_input.columns if (('covariate_' in column) | ('condition' in column) | ('procedure' in column) | ('vaccinated' in column) | ('risk_factor' in column)) & ('LVCF' in column) & ('exclusion' not in column)]
    print('covariates', covariate_columns)

    # Combine all covariate columns into a single list
    full_predictors = demographics + bmi_columns + covariate_columns
    full_predictors = [column for column in full_predictors if column in match_input.columns]

    # Filter df to the columns we need
    match_input = match_input.select(['person_id','day','treatment'] + full_predictors)

    ### Merge the two datasets; create the pre-matching and the post-matching dataset
    df = match_input.join(match_output, on = ['person_id','day'], how = 'left')
    df_pre_match = df.toPandas()
    df_post_match = df.where(expr('subclass IS NOT NULL')).toPandas()

    ############ THERE IS A STUPID DATA TYPE ISSUE - WE NEED THIS CODE (IT IS HAPPENING FOR THE AGE VARIABLE)
    string_columns = list(df_pre_match.select_dtypes('O').columns)
    string_columns = [col for col in string_columns if col not in ['person_id','date','subclass']]
    for stringcol in string_columns:
        print('column: ',stringcol)
        df_pre_match[stringcol] = df_pre_match[stringcol].astype(float)
        df_post_match[stringcol] = df_post_match[stringcol].astype(float)

    # Set up empty lists for before matching and postmatching
    prematched_means = []
    matched_means = []

    # Loop through our predictors and calculate balance
    for variable in full_predictors:
        print(variable)
        
        # Calculate prematched mean
        treatment_mean = df_pre_match.query('{} == 1'.format(treatment))[variable].mean()
        control_mean = df_pre_match.query('{} == 0'.format(treatment))[variable].mean()
        treatment_std = df_pre_match.query('{} == 1'.format(treatment))[variable].std()
        control_std = df_pre_match.query('{} == 0'.format(treatment))[variable].std()
        difference = treatment_mean - control_mean
        standardized_bias = difference / treatment_std
        prematched_means.append(np.abs(standardized_bias))

        # Calculate the postmatched mean
        treatment_matched_mean = df_post_match.query('{} == 1'.format(treatment))[variable].mean()
        control_matched_mean = df_post_match.query('{} == 0'.format(treatment))[variable].mean()
        treatment_matched_std = df_post_match.query('{} == 1'.format(treatment))[variable].std()
        control_matched_std = df_post_match.query('{} == 0'.format(treatment))[variable].std()
        matched_difference = treatment_matched_mean - control_matched_mean
        matched_standardized_bias = matched_difference / treatment_matched_std
        matched_means.append(np.abs(matched_standardized_bias))
        
    # Convert to data frame
    sb_df = pd.DataFrame({'variable': full_predictors,
                        'prematched_SB':prematched_means,
                        'matched_SB':matched_means})

    return sb_df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.69027164-230c-4bfa-b385-accf3b2b0482"),
    Enriched_Death_Table_Final=Input(rid="ri.foundry.main.dataset.67ebd4b5-1028-4053-8924-06ce93a15509"),
    Vaccine_fact_de_identified=Input(rid="ri.foundry.main.dataset.327d3991-44f0-4038-b778-761669fa6eb9"),
    cohort_code=Input(rid="ri.foundry.main.dataset.b6450c6b-55d3-41e3-8208-14fa0780cd41")
)
def vaccines_filtered(cohort_code,Vaccine_fact_de_identified, Enriched_Death_Table_Final):

    persons = cohort_code.select('person_id')
    
    # Vaccine_fact_de_identified table is 1 row per patient. Separate columns for each vaccine and date. Join to our person table
    vax_df = Vaccine_fact_de_identified.select('person_id', '1_vax_date', '2_vax_date', '3_vax_date', '4_vax_date', '5_vax_date') \
        .join(persons, 'person_id', 'inner')

    # create separate tables for each vaccine dose - two columns: person_id and the date of that vax. Each table is filtered to patients who only had that vaccine
    first_dose = vax_df.select('person_id', '1_vax_date') \
        .withColumnRenamed('1_vax_date', 'date') \
        .where(col('date').isNotNull())
    second_dose = vax_df.select('person_id', '2_vax_date') \
        .withColumnRenamed('2_vax_date', 'date') \
        .where(col('date').isNotNull())        
    third_dose = vax_df.select('person_id', '3_vax_date') \
        .withColumnRenamed('3_vax_date', 'date') \
        .where(col('date').isNotNull())
    fourth_dose = vax_df.select('person_id', '4_vax_date') \
        .withColumnRenamed('4_vax_date', 'date') \
        .where(col('date').isNotNull())
    fifth_dose = vax_df.select('person_id', '5_vax_date') \
        .withColumnRenamed('5_vax_date', 'date') \
        .where(col('date').isNotNull())
    

    # Perform outer join between these tables - this will STACK each distinct person_id, date row from the 4 tables above
    df = first_dose.join(second_dose, on=['person_id', 'date'], how='outer') \
        .join(third_dose, on=['person_id', 'date'], how='outer') \
        .join(fourth_dose, on=['person_id', 'date'], how='outer').join(fifth_dose, on=['person_id', 'date'], how='outer').distinct()

    # Make a row of ones to indicate that person was vaxed
    df = df.withColumn('had_vaccine_administered', lit(1))

    # add a column to indicate row number 
    df = df.withColumn(
        'vaccine_dose_number', row_number().over(Window.partitionBy('person_id').orderBy('date'))
    )

    return df

#################################################
## Global imports and functions included below ##
#################################################

from pyspark.sql import functions as F    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.09c6fdb9-33f0-4d00-b71d-433cdd927eac"),
    cohort_code=Input(rid="ri.foundry.main.dataset.b6450c6b-55d3-41e3-8208-14fa0780cd41"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    measurements_filtered=Input(rid="ri.foundry.main.dataset.2829da0b-d0cc-4a0e-8ecb-d8118849f408"),
    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.5af2c604-51e0-4afa-b1ae-1e5fa2f4b905")
)
# This query will output 1 row per patient only; and aims to identify the date COVID-related ED visits; PERTAINING TO THEIR FIRST DIAGNOSIS ONLY!

def visits_filtered(cohort_code, microvisits_to_macrovisits, concept_set_members, measurements_filtered):

    # First, we will set up a boolean to determine if we will use the likely hospitalization flag to identify hospitalizations (instead of just using macrovisits)
    use_likely_hosp_flag = True
    
    # Filter to our cohort of interest. Calculate a new column that is the number of days between a positive test and diagnosis (if both were done). This value will be negative if the diagnosis occurred after the positive test [recall these columns already exist in cohort_code]
    persons = cohort_code.select('person_id', 'COVID_first_PCR_or_AG_lab_positive', 'COVID_first_diagnosis_date', 'COVID_first_poslab_or_diagnosis_date').withColumn('lab_minus_diagnosis_date', datediff('COVID_first_PCR_or_AG_lab_positive','COVID_first_diagnosis_date'))
    
    #filter macrovisit table to only cohort patients    
    df = microvisits_to_macrovisits.select('person_id','visit_start_date','visit_concept_id','macrovisit_start_date','macrovisit_end_date','likely_hospitalization').join(persons,'person_id','inner')  
    
    # Take the concept set members table filtered to most recent concepts only
    concepts_df = concept_set_members.select('concept_set_name', 'is_most_recent_version', 'concept_id').where(col('is_most_recent_version')=='true')  

    # Make a list of concepts that relate to ED VISITS concept sets (PASC)
    ED_concept_ids = list(concepts_df.where((concepts_df.concept_set_name=="[PASC] ED Visits") & (concepts_df.is_most_recent_version=='true')).select('concept_id').toPandas()['concept_id'])
    
    ###### CREATE A TABLE TO IDENTIFY ONLY EMEREGENCY-DEPT RELATED VISITS (NOT HOSPITALIZATIONS)
    # Filter the macrovisits table above to visits that are ED-related (visit concept is one of our ED_concept_ids)
    # We will have multiple rows per patient, each referring to a ED-type visit
    df_ED = df.where(df.macrovisit_start_date.isNull() & (df.visit_concept_id.isin(ED_concept_ids)))
    
    # Caclulate the number of days in between a [FIRST] positive test and the visit start date. The row will be negative for ED visits occurring AFTER the positive test
    df_ED = df_ED.withColumn('lab_minus_ED_visit_start_date', datediff('COVID_first_PCR_or_AG_lab_positive','visit_start_date'))
    
    """
    (1) If covid_associated_ED_or_hosp_requires_lab_AND_diagnosis = True:
    This section will use our ED VISITS table (df_ed) to determine if the ED visits are COVID-related in line with CDC defintion. 
    Specifically - this is when an ED visit occurs within 16 days of a positive test AND the diagnosis also occurs within that time frame. 
    This will only pertain to patients who DID receive a diagnosis, and it occurred within that 16 day window. 
    
    If covid_associated_ED_or_hosp_requires_lab_AND_diagnosis = False:
    We will create a data frame with ED-related visits that occurred within 16 days following a positive test OR diagnosis, ignoring the condition that diagnosis must also have occurred in that interval. 
    Thus patients who didn't get diagnosed are also included here [as long as they had ED visits in that 16 day window]
    """
    
    #### IF WE WANT TO APPLY THE CDC definition - CHANGE TO TRUE (note we have changed the window from 16 to 14 days)
    covid_associated_ED_or_hosp_requires_lab_AND_diagnosis = True
    num_days_before_index = 1
    num_days_after_index = 14
    
    
    # If the number of days [between positive test and ED visit] is between -16 and 1, then mark 1 for "covid_pcr_or_ag_associated_ED_only_visit"
    # Then for those patients: if number of days [between the positive test and diagnosis] is ALSO between -16 and 1, then mark 1 for "COVID_lab_positive_and_diagnosed_ED_visit". These are patients who had a positive test and then were diagnosed within the ER. These are [COVID-related ED visits]. 
    # Filter to those rows. Rename the 'visit_start_date' to 'covid_ED_only_start_date' 
    # Final result contains person_id and covid_ED_only_start_date
    
    if covid_associated_ED_or_hosp_requires_lab_AND_diagnosis:
        df_ED = (df_ED.withColumn('covid_pcr_or_ag_associated_ED_only_visit', when(col('lab_minus_ED_visit_start_date').between(-num_days_after_index, num_days_before_index), 1).otherwise(0)).withColumn('COVID_lab_positive_and_diagnosed_ED_visit', when((col('covid_pcr_or_ag_associated_ED_only_visit')==1) & (col('lab_minus_diagnosis_date').between(-num_days_after_index,num_days_before_index)), 1).otherwise(0)).where(col('COVID_lab_positive_and_diagnosed_ED_visit')==1).withColumnRenamed('visit_start_date','covid_ED_only_start_date').select('person_id', 'covid_ED_only_start_date').dropDuplicates())
                
    # otherwise, first calculate the number of days between EITHER [FIRST positive test/positive diagnosis] and ED visit. The value is negative for visits after positive test/diagnosis
    # The CDC ACTUALLY defines hospitalization as 14 days between a SARS-CoV-2 PCR/AG test - it does NOT require a diagnosis; 
    else:
        df_ED = df_ED.withColumn("earliest_index_minus_ED_start_date", datediff("COVID_first_poslab_or_diagnosis_date","visit_start_date"))
        
        # When the visit occurred 16 days after or 1 day before the positive test then mark 1. This indicates an ED-visit ASSOCIATED with being COVID-positive. 
        # Note, we do NOT include the conditional that the patient was also diagnosed with COVID in that period. 
        # Filter to those rows. Rename the 'visit_start_date' to 'covid_ED_only_start_date' 
        df_ED = (df_ED.withColumn("covid_lab_or_dx_associated_ED_only_visit", when(col('earliest_index_minus_ED_start_date').between(-num_days_after_index,num_days_before_index), 1).otherwise(0)).where(col('covid_lab_or_dx_associated_ED_only_visit')==1).withColumnRenamed('visit_start_date','covid_ED_only_start_date').select('person_id', 'covid_ED_only_start_date' ).dropDuplicates())
        
    
    ####### CREATE A TABLE TO IDENTIFY THE HOSPITALIZATIONS - THESE ARE MACROVISITS
    # Back to the macrovisits table (df) merged with cohort_code [persons] - first filter to macrovisit rows (where macrovisit_start_date is not null)
    # Then calculate the number of days between a visit and the first positive test
    #### PART 1 - FIRST IDENTIFY MACROVISITS THAT ARE LIKELY_HOSPITALIZATIONS
    if use_likely_hosp_flag == True:
        df_hosp = df.where((col('macrovisit_start_date').isNotNull()) & (col('likely_hospitalization') == 1))
        df_hosp = df_hosp.withColumn("lab_minus_hosp_start_date", datediff("COVID_first_PCR_or_AG_lab_positive","macrovisit_start_date"))
    else:
        df_hosp = df.where(col('macrovisit_start_date').isNotNull())
        df_hosp = df_hosp.withColumn("lab_minus_hosp_start_date", datediff("COVID_first_PCR_or_AG_lab_positive","macrovisit_start_date")) 
    
    ##### PART 2: a) again, if we are interested in the CDC definition of COVID-19 related macrovisits use the following code.
    # This time we are interested in COVID-related HOSPITALIZATIONS not ED visits. 
    # If the hospital visit (non-ED) occurred in that -1 to 16 day window after the positive test then mark 1. 
    # Then among those patients, if the diagnosis ALSO occurred in that -1 to 16 day window from the test then mark 1.
    # Rename the column macrovisit_start_date to covid_hospitalization_start_date. Do the same for the end_date
    # Filter to just the person_id, and these two date columns. WE WILL END UP WITH 1 ROW PER VISIT
    # The table df_hosp will thus only be patients with a COVID-related-hospitalization based on the definition; 
    
    if covid_associated_ED_or_hosp_requires_lab_AND_diagnosis:

        # The code below requires the hospitalization to occur within 14 days of test AND the diagnosis to occur within 14 days of testing;  
        # The diagnosis can occur after the hospitalization if that hospitalization occurs within 14 days; 
        df_hosp = (df_hosp.withColumn("covid_pcr_or_ag_associated_hospitalization", when(col('lab_minus_hosp_start_date').between(-num_days_after_index,num_days_before_index), 1).otherwise(0)).withColumn("COVID_lab_positive_and_diagnosed_hospitalization", when((col('covid_pcr_or_ag_associated_hospitalization')==1) & (col('lab_minus_diagnosis_date').between(-num_days_after_index,num_days_before_index)), 1).otherwise(0)).where(col('COVID_lab_positive_and_diagnosed_hospitalization')==1).withColumnRenamed('macrovisit_start_date','covid_hospitalization_start_date').withColumnRenamed('macrovisit_end_date','covid_hospitalization_end_date').select('person_id', 'covid_hospitalization_start_date', 'covid_hospitalization_end_date').dropDuplicates())
                
    # If we are not interested in the CDC definition and we just want COVID-related macrovisits (hospitalizations) then use the following code
    # First calculate the number of days between each visit in df_hosp and the patient's first positive test OR diagnosis. The value will be negative for visits after the first diagnosis
    
    else:
        df_hosp = df_hosp.withColumn("earliest_index_minus_hosp_start_date", datediff("COVID_first_poslab_or_diagnosis_date","macrovisit_start_date")) 

        # If the macrovisit occurred in that 16 day window after the test/diagnosis then mark 1. Filter to those patients
        # Rename the macrovisit_start_date to covid_hospitalization_start_date
        df_hosp = (df_hosp.withColumn("covid_lab_or_diagnosis_associated_hospitilization", when(col('earliest_index_minus_hosp_start_date').between(-num_days_after_index,num_days_before_index), 1).otherwise(0)).where(col('covid_lab_or_diagnosis_associated_hospitilization')==1).withColumnRenamed('macrovisit_start_date','covid_hospitalization_start_date').withColumnRenamed('macrovisit_end_date','covid_hospitalization_end_date').select('person_id', 'covid_hospitalization_start_date', 'covid_hospitalization_end_date').dropDuplicates())
 
    # Join the full macrovisits table [df] to our tables for COVID-related ED visits and COVID-related macrovisits using outer join
    # This will ensure a patient has a row per every visit (from df), for every ED visit (from df_ED) and for every hosp_visit 
    df = df.join(df_ED, on = 'person_id', how = 'outer')
    df = df.join(df_hosp, on = 'person_id', how = 'outer')
    
    # Each patient now has multiple columns for different hospital dates: hosp visit, COVID-related ED visit date, and COVID-related hosp visit date. Group by patient to take the EARLIEST occurrence of the COVID-related ED visit start date, AND the COVID-related hospitalization start AND end dates
    df = df.groupby('person_id').agg(
    min('covid_ED_only_start_date').alias('first_COVID_ED_only_start_date'),
    min('covid_hospitalization_start_date').alias('first_COVID_hospitalization_start_date'),
    min('covid_hospitalization_end_date').alias('first_COVID_hospitalization_end_date'))

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e1a48189-26ee-4b58-9160-fa38aaafa5d3"),
    Sdoh_data=Input(rid="ri.foundry.main.dataset.7ac9db0e-055c-4c68-a666-1ec1f6da006d"),
    cohort_facts_visit_level_paxlovid=Input(rid="ri.foundry.main.dataset.63c6f325-61d9-4e4f-acc2-2e9e6c301adc")
)
def visits_simpler(cohort_facts_visit_level_paxlovid, Sdoh_data):
    cohort_facts_visit_level_paxlovid = cohort_facts_visit_level_paxlovid

    drop_patients_without_sdoh = False

    # Get the postal code back
    facts = cohort_facts_visit_level_paxlovid
    
    # Vaccination
    facts = facts.withColumnRenamed('had_vaccine_administered','vaccinated').withColumnRenamed('vaccine_dose_number','vaccinated_dose_count')

    # Rename column
    if 'pde5_inhibitor' in facts.columns:
        facts = facts.withColumnRenamed('pde5_inhibitor','pde5_inhibitor_contraindication')

    # Join the SODH - we only want the SODH index
    facts = facts.join(Sdoh_data.select('person_id','Five_Digit_Zip','sdoh2_by_preferred_county').withColumnRenamed('Five_Digit_Zip','postal_code'), on = 'person_id', how = 'left')
    
    if drop_patients_without_sdoh:
        facts = facts.dropna(subset = ['sdoh2_by_preferred_county'])

    # Group the relatively contraindicated drugs - into severe and moderate
    severe = ['anti_arrhythmia_relatively_contraindicated','anticoagulants_relatively_contraindicated','cardiovascular_agents_contraindicated','narcotics_relatively_contraindicated','sgc_stimulator_relatively_contraindicated','alpha1_adrenoceptor_antagonist_relatively_contraindicated','endothilin_receptor_antagonists_relatively_contraindicated','hepatitis_c_direct_acting_antivirals_relatively_contraindicated','jak_inhibitors_relatively_contraindicated','immunosuppressants_relatively_contraindicated','pde5_inhibitors_relatively_contraindicated','sedatives_relatively_contraindicated','migraine_medication_relatively_contraindicated','muscarinic_receptor_antagonist_relatively_contraindicated']
    severe = [column for column in severe if column in facts.columns]
    severe_string = ','.join(severe)
    
    moderate = ['antipsychotics_relatively_contraindicated','anti_cancer_relatively_contraindicated','antimyobacterial_relatively_contraindicated','antifungal_relatively_relatively_contraindicated','corticosteroids_relatively_contraindicated','anti_infective_relatively_contraindicated','anticonvulsant_relatively_contraindicated','neuropsychiatric_agents_relatively_contraindicated','antidepressants_relatively_contraindicated','calcium_channel_blockers_relatively_contraindicated','dpp4_inhibitors_relatively_contraindicated']
    moderate = [column for column in moderate if column in facts.columns]
    moderate_string = ','.join(moderate)

    # Create the columns for the drug interactions
    facts = facts.withColumn('severe_relatively_contraindicated', expr('GREATEST({})'.format(severe_string))).drop(*severe)
    facts = facts.withColumn('moderate_relatively_contraindicated', expr('GREATEST({})'.format(moderate_string))).drop(*moderate)

    return facts

    

    
    

    

    

