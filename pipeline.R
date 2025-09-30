

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.4c36dc00-4cc7-45ad-aee7-846b1481c824"),
    coxreg_prep_matching_composite_death_hosp=Input(rid="ri.foundry.main.dataset.1957ac02-7a23-44b0-8bb1-ecd8fff34c51")
)
# Competing Risk Cox Regression Bootstrap Survival Curves (8ad454f5-eb5d-4956-8d8e-8671a5ae118c): v0
Competing_Risk_CoxRegression_Bootstrapping_Hospitalization <- function(coxreg_prep_matching_composite_death_hosp) {

    library('dplyr')
    library('survival')
    library('mstate')
    
    df = coxreg_prep_matching_composite_death_hosp
    print('full df size')
    print(dim(df))

    ##### PERFORM BOOTSTRAMP SAMPLING - FIT THE MODEL ON EACH BOOTSTRAP; APPLY IT TO THE PREDICTION DATASET; SAVE THE PREDICTION DATASET
    # final_results = data.frame()
    datalist = list()

    # Set up the prediction dataset that we will apply on every fitted model
    prediction_df <- data.frame(treatment=c(1, 0))
    
    # What are the list of subclasses? 
    subclasses <- unique(df$subclass) # What are the unique subclasses?
    min_subclass <- min(subclasses)
    max_subclass <- max(subclasses)
    n_matched_pairs <- length(subclasses) # How many matched pairs are there altogether (= number of subclasses)

    # Set up the loop with n bootstraps
    n_bootstraps <- 100
    for(i in seq(1:n_bootstraps))
    {
        set.seed(i)
        print(i)

        # # First take a bootstrap sample of the 1 row PP dataset
        # paxlovid_data_sample <- trial1 %>% sample_frac(size = 0.663, replace = TRUE)

        # First create a random sample of match pair IDs
        indexes <- sample(subclasses, n_matched_pairs, replace=TRUE)
        print(length(indexes))

        #### NEW STEP - convert to a data frame 
        subclass_df <- data.frame(subclass = indexes)

        # # First take a bootstrap sample from the original dataset > i.e., filter to patients in the selected subclasses above
        # paxlovid_data_sample <- df[df$subclass %in% indexes, ]
        
        #### Alternative step - Merge the subclass_df to our main_df: this will be a many to 1 join
        paxlovid_data_sample <- subclass_df %>% inner_join(df, by = 'subclass')
        
        # What are the dimensions?
        print(dim(paxlovid_data_sample))

        # Perform the crprep on that sample; we should expand person_id_trial (to expand patients WITHIN each trial)
        ## Note: person_id_trial will be kept, even if we don't include it in the keep options
        result <- crprep("hosp_death_competing_time14", "hosp_death_competing14", data = paxlovid_data_sample, trans=c(1, 2), cens = 0, strata = "treatment", keep = c('subclass','person_id'))
        print(colnames(result))

        # Set up the model object and input the sample data into it. 
        cox_regression <- coxph(Surv(Tstart, Tstop, status==1) ~ treatment, 
        weights = weight.cens, 
        data = result, 
        subset = failcode==1, 
        cluster = subclass, 
        x = TRUE)

        # Print the results of the model
        print(summary(cox_regression))

        # Get predicted survival curves; input the prediction dataset
        survival_curves <- survfit(cox_regression, newdata = prediction_df)

        # Convert the predicted survival curve output into a dataframe
        # The result data frame will produce 3 columns: time, treatment, and control. Each row will be the survival time. 
        survival_curves_df <- data.frame(time = survival_curves$time,
        surv = survival_curves$surv
        )

        # Add a column to indicate the bootstrap
        survival_curves_df$bootstrap <- i

        # Append the survival analysis results to our list
        datalist[[i]] <- survival_curves_df

    }

    # Repeat for the overall dataset to get point estimate
    result <- crprep("hosp_death_competing_time14", "hosp_death_competing14", data = df, trans=c(1, 2), cens = 0, strata = "treatment", keep = c('subclass','person_id'))
    print(colnames(result))

    # Set up the model object and input the sample data into it. 
    cox_regression <- coxph(Surv(Tstart, Tstop, status==1) ~ treatment, 
    weights = weight.cens, 
    data = result, 
    subset = failcode==1, 
    cluster = subclass, 
    x = TRUE)

    # Print the results of the model
    print(summary(cox_regression))

    # Get predicted survival curves; input the prediction dataset
    survival_curves <- survfit(cox_regression, newdata = prediction_df)

    # Convert the predicted survival curve output into a dataframe
    # The result data frame will produce 3 columns: time, treatment, and control. Each row will be the survival time. 
    survival_curves_df <- data.frame(time = survival_curves$time,
    surv = survival_curves$surv
    )

    # Add a column to indicate the bootstrap
    survival_curves_df$bootstrap <- 999

    # Append the survival analysis results to our list
    datalist[[i]] <- survival_curves_df

    ########### COMBINE ALL RESULTS #####################################
    # Bind the rows into a full dataframe
    final_results <- dplyr::bind_rows(datalist)
    colnames(final_results) <- c('time','treatment','control','bootstrap')

    # Return the 
    return(final_results)
    
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f6b4bf63-2ee1-43b9-a939-14ce9d729f66"),
    paxlovid_long_covid_coxreg_prep=Input(rid="ri.foundry.main.dataset.1befb2f1-8bd9-4a7c-ab66-d8ebb9c61396")
)
# Competing Risk Cox Regression Bootstrap Survival Curves (8ad454f5-eb5d-4956-8d8e-8671a5ae118c): v0
Competing_Risk_CoxRegression_Bootstrapping_LongCOVID <- function(paxlovid_long_covid_coxreg_prep) {

    library('dplyr')
    library('survival')
    library('mstate')
    
    df = paxlovid_long_covid_coxreg_prep
    print('full df size')
    print(dim(df))

    ##### PERFORM BOOTSTRAMP SAMPLING - FIT THE MODEL ON EACH BOOTSTRAP; APPLY IT TO THE PREDICTION DATASET; SAVE THE PREDICTION DATASET
    # final_results = data.frame()
    datalist = list()

    # Set up the prediction dataset that we will apply on every fitted model
    prediction_df <- data.frame(treatment=c(1, 0))
    
    # What are the list of subclasses? 
    subclasses <- unique(df$subclass) # What are the unique subclasses?
    min_subclass <- min(subclasses)
    max_subclass <- max(subclasses)
    n_matched_pairs <- length(subclasses) # How many matched pairs are there altogether (= number of subclasses)

    # Set up the loop with n bootstraps
    n_bootstraps <- 100
    for(i in seq(1:n_bootstraps))
    {
        set.seed(i)
        print(i)

        # # First take a bootstrap sample of the 1 row PP dataset
        # paxlovid_data_sample <- trial1 %>% sample_frac(size = 0.663, replace = TRUE)

        # First create a random sample of match pair IDs
        indexes <- sample(subclasses, n_matched_pairs, replace=TRUE)
        print(length(indexes))

        #### NEW STEP - convert to a data frame 
        subclass_df <- data.frame(subclass = indexes)

        # # First take a bootstrap sample from the original dataset > i.e., filter to patients in the selected subclasses above
        # paxlovid_data_sample <- df[df$subclass %in% indexes, ]
        
        #### Alternative step - Merge the subclass_df to our main_df: this will be a many to 1 join
        paxlovid_data_sample <- subclass_df %>% inner_join(df, by = 'subclass')
        
        # What are the dimensions?
        print(dim(paxlovid_data_sample))

        # Perform the crprep on that sample; we should expand person_id_trial (to expand patients WITHIN each trial)
        ## Note: person_id_trial will be kept, even if we don't include it in the keep options
        result <- crprep("event_time", "event", data = paxlovid_data_sample, trans=c(1, 2), cens = 0, strata = "treatment", keep = c('subclass','person_id'))
        print(colnames(result))

        # Set up the model object and input the sample data into it. 
        cox_regression <- coxph(Surv(Tstart, Tstop, status==1) ~ treatment, 
        weights = weight.cens, 
        data = result, 
        subset = failcode==1, 
        cluster = subclass, 
        x = TRUE)

        # Print the results of the model
        print(summary(cox_regression))

        # Get predicted survival curves; input the prediction dataset
        survival_curves <- survfit(cox_regression, newdata = prediction_df)

        # Convert the predicted survival curve output into a dataframe
        # The result data frame will produce 3 columns: time, treatment, and control. Each row will be the survival time. 
        survival_curves_df <- data.frame(time = survival_curves$time,
        surv = survival_curves$surv
        )

        # Add a column to indicate the bootstrap
        survival_curves_df$bootstrap <- i

        # Append the survival analysis results to our list
        datalist[[i]] <- survival_curves_df

    }

    # Repeat for the overall dataset to get point estimate
    result <- crprep("event_time", "event", data = df, trans=c(1, 2), cens = 0, strata = "treatment", keep = c('subclass','person_id'))
    print(colnames(result))

    # Set up the model object and input the sample data into it. 
    cox_regression <- coxph(Surv(Tstart, Tstop, status==1) ~ treatment, 
    weights = weight.cens, 
    data = result, 
    subset = failcode==1, 
    cluster = subclass, 
    x = TRUE)

    # Print the results of the model
    print(summary(cox_regression))

    # Get predicted survival curves; input the prediction dataset
    survival_curves <- survfit(cox_regression, newdata = prediction_df)

    # Convert the predicted survival curve output into a dataframe
    # The result data frame will produce 3 columns: time, treatment, and control. Each row will be the survival time. 
    survival_curves_df <- data.frame(time = survival_curves$time,
    surv = survival_curves$surv
    )

    # Add a column to indicate the bootstrap
    survival_curves_df$bootstrap <- 999

    # Append the survival analysis results to our list
    datalist[[i]] <- survival_curves_df

    ########### COMBINE ALL RESULTS #####################################
    # Bind the rows into a full dataframe
    final_results <- dplyr::bind_rows(datalist)
    colnames(final_results) <- c('time','treatment','control','bootstrap')

    # Return the 
    return(final_results)
    
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f3f5ddb5-9fe2-4960-895d-af0bc73d4503"),
    coxreg_prep_matching_death=Input(rid="ri.foundry.main.dataset.22c99e32-2f92-42c0-b2ec-412442b2c731")
)
# Bootstrapped Cox Regression Survival Curves (b1bf5bac-cbd5-40da-af03-93e0f3d7120a): v2
CoxRegression_Bootstrapping_Death <- function(coxreg_prep_matching_death) {

    library('dplyr')
    library('survival')
    # library('mstate')
    
    df = coxreg_prep_matching_death
    print('full df size')
    print(dim(df))

    ##### PERFORM BOOTSTRAMP SAMPLING - FIT THE MODEL ON EACH BOOTSTRAP; APPLY IT TO THE PREDICTION DATASET; SAVE THE PREDICTION DATASET
    # final_results = data.frame()
    datalist = list()

    # Set up the prediction dataset that we will apply on every fitted model
    prediction_df <- data.frame(treatment=c(1, 0))
    
    # What are the list of subclasses? 
    subclasses <- unique(df$subclass) # What are the unique subclasses?
    min_subclass <- min(subclasses)
    max_subclass <- max(subclasses)
    n_matched_pairs <- length(subclasses) # How many matched pairs are there altogether (= number of subclasses)

    # Set up the loop with n bootstraps
    n_bootstraps <- 100
    for(i in seq(1:n_bootstraps))
    {
        set.seed(i)
        print(i)

        # First create a random sample of match pair IDs
        indexes <- sample(subclasses, n_matched_pairs, replace=TRUE)

        #### NEW STEP - convert to a data frame 
        subclass_df <- data.frame(subclass = indexes)
        
        #### Alternative step - Merge the subclass_df to our main_df: this will be a many to 1 join
        paxlovid_data_sample <- subclass_df %>% inner_join(df, by = 'subclass')

        # Fit the COXREG model on each bootstrap; 
        cox_regression <- coxph(Surv(death_time14, death14) ~ treatment + cluster(subclass), data=paxlovid_data_sample)
        # print(summary(cox_regression))

        # Get predicted survival curves; input the prediction dataset
        survival_curves <- survfit(cox_regression, newdata = prediction_df)

        # Convert the predicted survival curve output into a dataframe
        # The result data frame will produce 3 columns: time, treatment, and control. Each row will be the survival time. 
        survival_curves_df <- data.frame(time = survival_curves$time,
        surv = survival_curves$surv
        )

        # Add a column to indicate the bootstrap
        survival_curves_df$bootstrap <- i

        # Append the survival analysis results to our list
        datalist[[i]] <- survival_curves_df

    }

    # Repeat for the point estimate on the full dataset
    cox_regression <- coxph(Surv(death_time14, death14) ~ treatment + cluster(subclass), data=df)

    # Get predicted survival curves; input the prediction dataset
    survival_curves <- survfit(cox_regression, newdata = prediction_df)

    # Convert the predicted survival curve output into a dataframe with 3 columns: time, treatment, and control. Each row will be the survival time. 
    survival_curves_df <- data.frame(time = survival_curves$time,
    surv = survival_curves$surv
    )

    # Add a column to indicate the bootstrap
    survival_curves_df$bootstrap <- 999

    # Append the survival analysis results to our list
    datalist[[i]] <- survival_curves_df

    
    
    # Bind the rows into a full dataframe
    final_results <- dplyr::bind_rows(datalist)
    colnames(final_results) <- c('timeline','treatment','control','bootstrap')

    # Return the 
    return(final_results)
    
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.17846d20-9287-43c6-95ca-0a833ffdcc9e"),
    coxreg_prep_matching_deathhosp=Input(rid="ri.foundry.main.dataset.edd4f694-66db-4176-bde5-06d64e098f7b")
)
# Bootstrapped Cox Regression Survival Curves (b1bf5bac-cbd5-40da-af03-93e0f3d7120a): v2
CoxRegression_Bootstrapping_DeathHosp <- function(coxreg_prep_matching_deathhosp) {

    library('dplyr')
    library('survival')
    # library('mstate')
    
    df = coxreg_prep_matching_deathhosp
    print('full df size')
    print(dim(df))

    ##### PERFORM BOOTSTRAMP SAMPLING - FIT THE MODEL ON EACH BOOTSTRAP; APPLY IT TO THE PREDICTION DATASET; SAVE THE PREDICTION DATASET
    # final_results = data.frame()
    datalist = list()

    # Set up the prediction dataset that we will apply on every fitted model
    prediction_df <- data.frame(treatment=c(1, 0))
    
    # What are the list of subclasses? 
    subclasses <- unique(df$subclass) # What are the unique subclasses?
    min_subclass <- min(subclasses)
    max_subclass <- max(subclasses)
    n_matched_pairs <- length(subclasses) # How many matched pairs are there altogether (= number of subclasses)

    # Set up the loop with n bootstraps
    n_bootstraps <- 100
    for(i in seq(1:n_bootstraps))
    {
        set.seed(i)
        print(i)

        # First create a random sample of match pair IDs
        indexes <- sample(subclasses, n_matched_pairs, replace=TRUE)

        #### NEW STEP - convert to a data frame 
        subclass_df <- data.frame(subclass = indexes)
        
        #### Alternative step - Merge the subclass_df to our main_df: this will be a many to 1 join
        paxlovid_data_sample <- subclass_df %>% inner_join(df, by = 'subclass')

        # Fit the COXREG model on each bootstrap; 
        cox_regression <- coxph(Surv(composite_death_hosp_time14, composite_death_hosp14) ~ treatment + cluster(subclass), data=paxlovid_data_sample)
        # print(summary(cox_regression))

        # Get predicted survival curves; input the prediction dataset
        survival_curves <- survfit(cox_regression, newdata = prediction_df)

        # Convert the predicted survival curve output into a dataframe
        # The result data frame will produce 3 columns: time, treatment, and control. Each row will be the survival time. 
        survival_curves_df <- data.frame(time = survival_curves$time,
        surv = survival_curves$surv
        )

        # Add a column to indicate the bootstrap
        survival_curves_df$bootstrap <- i

        # Append the survival analysis results to our list
        datalist[[i]] <- survival_curves_df

    }

    # Repeat for the point estimate on the full dataset
    cox_regression <- coxph(Surv(composite_death_hosp_time14, composite_death_hosp14) ~ treatment + cluster(subclass), data=df)

    # Get predicted survival curves; input the prediction dataset
    survival_curves <- survfit(cox_regression, newdata = prediction_df)

    # Convert the predicted survival curve output into a dataframe with 3 columns: time, treatment, and control. Each row will be the survival time. 
    survival_curves_df <- data.frame(time = survival_curves$time,
    surv = survival_curves$surv
    )

    # Add a column to indicate the bootstrap
    survival_curves_df$bootstrap <- 999

    # Append the survival analysis results to our list
    datalist[[i]] <- survival_curves_df

    
    
    # Bind the rows into a full dataframe
    final_results <- dplyr::bind_rows(datalist)
    colnames(final_results) <- c('timeline','treatment','control','bootstrap')

    # Return the 
    return(final_results)
    
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.aa5bd8c3-32e8-49ff-b82f-31209e9773dd"),
    coxreg_prep_matching_composite=Input(rid="ri.foundry.main.dataset.ff6099cf-f640-4e55-a82b-5d99a236d3f2")
)
# Bootstrapped Cox Regression Survival Curves (b1bf5bac-cbd5-40da-af03-93e0f3d7120a): v2
CoxRegression_Bootstrapping_SevereIllness <- function(coxreg_prep_matching_composite) {

    library('dplyr')
    library('survival')
    # library('mstate')
    
    df = coxreg_prep_matching_composite
    print('full df size')
    print(dim(df))

    ##### PERFORM BOOTSTRAMP SAMPLING - FIT THE MODEL ON EACH BOOTSTRAP; APPLY IT TO THE PREDICTION DATASET; SAVE THE PREDICTION DATASET
    # final_results = data.frame()
    datalist = list()

    # Set up the prediction dataset that we will apply on every fitted model
    prediction_df <- data.frame(treatment=c(1, 0))
    
    # What are the list of subclasses? 
    subclasses <- unique(df$subclass) # What are the unique subclasses?
    min_subclass <- min(subclasses)
    max_subclass <- max(subclasses)
    n_matched_pairs <- length(subclasses) # How many matched pairs are there altogether (= number of subclasses)

    # Set up the loop with n bootstraps
    n_bootstraps <- 100
    for(i in seq(1:n_bootstraps))
    {
        set.seed(i)
        print(i)

        # First create a random sample of match pair IDs
        indexes <- sample(subclasses, n_matched_pairs, replace=TRUE)

        #### NEW STEP - convert to a data frame 
        subclass_df <- data.frame(subclass = indexes)
        
        #### Alternative step - Merge the subclass_df to our main_df: this will be a many to 1 join
        paxlovid_data_sample <- subclass_df %>% inner_join(df, by = 'subclass')

        # Fit the COXREG model on each bootstrap; 
        cox_regression <- coxph(Surv(composite_time14, composite14) ~ treatment + cluster(subclass), data=paxlovid_data_sample)
        # print(summary(cox_regression))

        # Get predicted survival curves; input the prediction dataset
        survival_curves <- survfit(cox_regression, newdata = prediction_df)

        # Convert the predicted survival curve output into a dataframe
        # The result data frame will produce 3 columns: time, treatment, and control. Each row will be the survival time. 
        survival_curves_df <- data.frame(time = survival_curves$time,
        surv = survival_curves$surv
        )

        # Add a column to indicate the bootstrap
        survival_curves_df$bootstrap <- i

        # Append the survival analysis results to our list
        datalist[[i]] <- survival_curves_df

    }

    # Repeat for the point estimate on the full dataset
    cox_regression <- coxph(Surv(composite_time14, composite14) ~ treatment + cluster(subclass), data=df)

    # Get predicted survival curves; input the prediction dataset
    survival_curves <- survfit(cox_regression, newdata = prediction_df)

    # Convert the predicted survival curve output into a dataframe with 3 columns: time, treatment, and control. Each row will be the survival time. 
    survival_curves_df <- data.frame(time = survival_curves$time,
    surv = survival_curves$surv
    )

    # Add a column to indicate the bootstrap
    survival_curves_df$bootstrap <- 999

    # Append the survival analysis results to our list
    datalist[[i]] <- survival_curves_df

    
    
    # Bind the rows into a full dataframe
    final_results <- dplyr::bind_rows(datalist)
    colnames(final_results) <- c('timeline','treatment','control','bootstrap')

    # Return the 
    return(final_results)
    
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.22a70e8c-0514-407d-9bdd-ae09ac1ab182"),
    coxreg_prep_matching_composite_death_hosp=Input(rid="ri.foundry.main.dataset.1957ac02-7a23-44b0-8bb1-ecd8fff34c51")
)
# Cox Regression Competing Risk (fb79e4c7-0488-4a51-9ae3-19b7fa4f809b): v0
coxreg_matching_competing_risk_hospitalization <- function(coxreg_prep_matching_composite_death_hosp) {

    DF = coxreg_prep_matching_composite_death_hosp

    library('dplyr')
    library('survival')
    library('mstate')
    # library('broom')

    # Expand the dataset - should be done for each distinct person-trial combination. Trans are the competing events. 1 = focal event. 2 = competing event
    long_df <- crprep("hosp_death_competing_time14", "hosp_death_competing14", data = DF, trans=c(1, 2), cens = 0, strata = "treatment", keep = c('subclass','person_id'))
    print(head(long_df))

    # NOTE: this creates a row_id variable called ID. Each "person-trial" is a distinct value

    # Perform the Cox regression to predict hospitalization. Cluster by matched_pair (some patients appear in separate trials)
    cox_regression_subclass <- coxph(Surv(Tstart, Tstop, status==1) ~ treatment, 
    weights = weight.cens, 
    data = long_df,
    subset = failcode==1,
    cluster = subclass,
    x = TRUE)

    cox_regression_patient <- coxph(Surv(Tstart, Tstop, status==1) ~ treatment, 
    weights = weight.cens, 
    data = long_df,
    subset = failcode==1,
    cluster = person_id,
    x = TRUE)

    cox_regression_row <- coxph(Surv(Tstart, Tstop, status==1) ~ treatment, 
    weights = weight.cens, 
    data = long_df,
    subset = failcode==1,
    cluster = id,
    x = TRUE)
    
    # # Fit the Cox regression model
    # cox_regression_subclass <- coxph(Surv(long_covid_time,  outcome) ~ treatment + cluster(subclass), data=df_overall) 
    # cox_regression_patient <- coxph(Surv(long_covid_time,  outcome) ~ treatment + cluster(person_id), data=df_overall) 
    # cox_regression_row <- coxph(Surv(long_covid_time,  outcome) ~ treatment + cluster(row_id), data=df_overall)
    
    # Get the variances
    variance_subclass <- cox_regression_subclass$var # variance1
    variance_patient <- cox_regression_patient$var # variance2
    variance_row <- cox_regression_row$var # variance3

    # Get pooled variance
    K1 = length(unique(long_df$subclass)) # of unique pairs
    K2 = length(unique(long_df$person_id)) # of unique patients
    K3 = length(unique(long_df$id)) # of unique rows
    
    adjusted_variance = ((K1 / (K1 - 1)) * variance_subclass) + ((K2 / (K2 - 1)) * variance_patient) - ((K3 / (K3 - 1)) * variance_row)
    adjusted_SE = sqrt(adjusted_variance)

    ##### OUTPUT THE FINAL RESULTS TABLE
    
    # Return the relative risk from the first model (we can use any model)
    rr_conf <- exp(cbind(RR = coef(cox_regression_subclass), confint(cox_regression_subclass)))
    rr_df <- as.data.frame(rr_conf)["treatment",] # Convert to data frame and extract row for the coefficient of interest (treatment)
    names(rr_df) <- c("RR", "2_5", "97_5")

    # Assign the row index (which are variable names) as a column, so we can see the coefficient for each variable
    rr_df$variable <- rownames(rr_df)
    print(rr_df)

    # Return the coefficient and P-value
    coef_pval <- summary(cox_regression_subclass)$coefficients
    coef_pval <- as.data.frame(coef_pval)["treatment",]
    coef_pval$variable <- rownames(coef_pval)
    print(coef_pval)

    # Merge the tables together
    final <- coef_pval %>% inner_join(rr_df, by = "variable")

    #### Calculate the adjusted lower and upper limit; Then assign them as columns
    estimate <- final[1, "coef"]
    adjusted_LL <- exp(estimate - 1.96 * adjusted_SE)
    adjusted_UL <- exp(estimate + 1.96 * adjusted_SE)

    final$adjusted_LL <- adjusted_LL
    final$adjusted_UL <- adjusted_UL

    # Rename columns so we can output the table
    final <- final %>% rename("HR" = "exp(coef)","SE" = "se(coef)","Robust_SE" = "robust se", "P" = "Pr(>|z|)", "LL" = "2_5", "UL" = "97_5","HR_correct" = "RR") %>% select(-c("HR_correct"))

    # return
    return(data.frame(final))
    
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f04db14f-e568-486a-913d-6e0a8d36623b"),
    paxlovid_long_covid_coxreg_prep=Input(rid="ri.foundry.main.dataset.1befb2f1-8bd9-4a7c-ab66-d8ebb9c61396")
)
# Cox Regression Competing Risk (fb79e4c7-0488-4a51-9ae3-19b7fa4f809b): v0
coxreg_matching_competing_risk_longcovid <- function(paxlovid_long_covid_coxreg_prep) {

    DF = paxlovid_long_covid_coxreg_prep

    library('dplyr')
    library('survival')
    library('mstate')
    # library('broom')

    # Expand the dataset - should be done for each distinct person-trial combination. Trans are the competing events. 1 = focal event. 2 = competing event
    long_df <- crprep("event_time", "event", data = DF, trans=c(1, 2), cens = 0, strata = "treatment", keep = c('subclass','person_id'))
    print(head(long_df))

    # NOTE: this creates a row_id variable called ID. Each "person-trial" is a distinct value

    # Perform the Cox regression to predict hospitalization. Cluster by matched_pair (some patients appear in separate trials)
    cox_regression_subclass <- coxph(Surv(Tstart, Tstop, status==1) ~ treatment, 
    weights = weight.cens, 
    data = long_df,
    subset = failcode==1,
    cluster = subclass,
    x = TRUE)

    cox_regression_patient <- coxph(Surv(Tstart, Tstop, status==1) ~ treatment, 
    weights = weight.cens, 
    data = long_df,
    subset = failcode==1,
    cluster = person_id,
    x = TRUE)

    cox_regression_row <- coxph(Surv(Tstart, Tstop, status==1) ~ treatment, 
    weights = weight.cens, 
    data = long_df,
    subset = failcode==1,
    cluster = id,
    x = TRUE)
    
    # # Fit the Cox regression model
    # cox_regression_subclass <- coxph(Surv(long_covid_time,  outcome) ~ treatment + cluster(subclass), data=df_overall) 
    # cox_regression_patient <- coxph(Surv(long_covid_time,  outcome) ~ treatment + cluster(person_id), data=df_overall) 
    # cox_regression_row <- coxph(Surv(long_covid_time,  outcome) ~ treatment + cluster(row_id), data=df_overall)
    
    # Get the variances
    variance_subclass <- cox_regression_subclass$var # variance1
    variance_patient <- cox_regression_patient$var # variance2
    variance_row <- cox_regression_row$var # variance3

    # Get pooled variance
    K1 = length(unique(long_df$subclass)) # of unique pairs
    K2 = length(unique(long_df$person_id)) # of unique patients
    K3 = length(unique(long_df$id)) # of unique rows
    
    adjusted_variance = ((K1 / (K1 - 1)) * variance_subclass) + ((K2 / (K2 - 1)) * variance_patient) - ((K3 / (K3 - 1)) * variance_row)
    adjusted_SE = sqrt(adjusted_variance)

    ##### OUTPUT THE FINAL RESULTS TABLE
    
    # Return the relative risk from the first model (we can use any model)
    rr_conf <- exp(cbind(RR = coef(cox_regression_subclass), confint(cox_regression_subclass)))
    rr_df <- as.data.frame(rr_conf)["treatment",] # Convert to data frame and extract row for the coefficient of interest (treatment)
    names(rr_df) <- c("RR", "2_5", "97_5")

    # Assign the row index (which are variable names) as a column, so we can see the coefficient for each variable
    rr_df$variable <- rownames(rr_df)
    print(rr_df)

    # Return the coefficient and P-value
    coef_pval <- summary(cox_regression_subclass)$coefficients
    coef_pval <- as.data.frame(coef_pval)["treatment",]
    coef_pval$variable <- rownames(coef_pval)
    print(coef_pval)

    # Merge the tables together
    final <- coef_pval %>% inner_join(rr_df, by = "variable")

    #### Calculate the adjusted lower and upper limit; Then assign them as columns
    estimate <- final[1, "coef"]
    adjusted_LL <- exp(estimate - 1.96 * adjusted_SE)
    adjusted_UL <- exp(estimate + 1.96 * adjusted_SE)

    final$adjusted_LL <- adjusted_LL
    final$adjusted_UL <- adjusted_UL

    # Rename columns so we can output the table
    final <- final %>% rename("HR" = "exp(coef)","SE" = "se(coef)","Robust_SE" = "robust se", "P" = "Pr(>|z|)", "LL" = "2_5", "UL" = "97_5","HR_correct" = "RR") %>% select(-c("HR_correct"))

    # return
    return(data.frame(final))
    
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.02d290e4-679c-4bc4-8950-2c8582620696"),
    coxreg_prep_matching_composite=Input(rid="ri.foundry.main.dataset.ff6099cf-f640-4e55-a82b-5d99a236d3f2")
)
# Cox Regression for Sequential Trial (Paxlovid) (0d360c3d-7df7-4ac4-9286-35e10c14df7a): v3
coxreg_matching_composite <- function(coxreg_prep_matching_composite) {
    
    # This node is used to identify the variances for the treatment effect after clustering by pair, person, and row
    # This will enable us to get standard errors adjusted for replacement in the matching process
    
    library('survival')
    library('dplyr')
    
    df_overall <- coxreg_prep_matching_composite

    # Fit the Cox regression model
    overall.cox.nnm.pair <- coxph(Surv(composite_time14, composite14) ~ treatment + cluster(subclass), data=df_overall) 
    overall.cox.nnm.subject <- coxph(Surv(composite_time14, composite14) ~ treatment + cluster(person_id), data=df_overall) 
    overall.cox.nnm.cross <- coxph(Surv(composite_time14, composite14) ~ treatment + cluster(row_id), data=df_overall)
    
    # Get the variances
    overall.var.pair.nnm <- overall.cox.nnm.pair$var # variance1
    overall.var.subject.nnm <- overall.cox.nnm.subject$var # variance2
    overall.var.cross.nnm <- overall.cox.nnm.cross$var # variance3

    # Get pooled variance
    K1 = length(unique(df_overall$subclass)) # of unique pairs
    K2 = length(unique(df_overall$person_id)) # of unique patients
    K3 = length(unique(df_overall$row_id)) # of unique rows
    adjusted_variance = ((K1 / (K1 - 1)) * overall.var.pair.nnm) + ((K2 / (K2 - 1)) * overall.var.subject.nnm) - ((K3 / (K3 - 1)) * overall.var.cross.nnm)
    adjusted_SE = sqrt(adjusted_variance)

    ##### OUTPUT THE FINAL RESULTS TABLE
    
    # Return the relative risk from the first model (we can use any model)
    rr_conf <- exp(cbind(RR = coef(overall.cox.nnm.pair), confint(overall.cox.nnm.pair)))
    # rr_df <- as.data.frame(rr_conf) # Convert to data frame. 
    rr_df <- as.data.frame(rr_conf)["treatment",] # Convert to data frame and extract row for the coefficient of interest (treatment)
    names(rr_df) <- c("RR", "2_5", "97_5")

    # Assign the row index (which are variable names) as a column, so we can see the coefficient for each variable
    rr_df$variable <- rownames(rr_df)
    print(rr_df)

    # Return the coefficient and P-value
    coef_pval <- summary(overall.cox.nnm.pair)$coefficients
    # coef_pval <- as.data.frame(coef_pval)
    coef_pval <- as.data.frame(coef_pval)["treatment",]
    coef_pval$variable <- rownames(coef_pval)
    print(coef_pval)

    # Merge the tables together
    final <- coef_pval %>% inner_join(rr_df, by = "variable")

    #### Calculate the adjusted lower and upper limit; Then assign them as columns
    estimate <- final[1, "coef"]
    adjusted_LL <- exp(estimate - 1.96 * adjusted_SE)
    adjusted_UL <- exp(estimate + 1.96 * adjusted_SE)

    final$adjusted_LL <- adjusted_LL
    final$adjusted_UL <- adjusted_UL

    # Rename columns so we can output the table
    final <- final %>% rename("HR" = "exp(coef)","SE" = "se(coef)","Robust_SE" = "robust se", "P" = "Pr(>|z|)", "LL" = "2_5", "UL" = "97_5","HR_correct" = "RR") %>% select(-c("HR_correct"))

    # return
    return(data.frame(final))
    
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.7a61c0b5-0bfa-4455-8e47-28de0483238b"),
    coxreg_prep_matching_death=Input(rid="ri.foundry.main.dataset.22c99e32-2f92-42c0-b2ec-412442b2c731")
)
# Cox Regression for Sequential Trial (Paxlovid) (0d360c3d-7df7-4ac4-9286-35e10c14df7a): v3
coxreg_matching_death <- function(coxreg_prep_matching_death) {
    
    # This node is used to identify the variances for the treatment effect after clustering by pair, person, and row
    # This will enable us to get standard errors adjusted for replacement in the matching process
    
    library('survival')
    library('dplyr')
    
    df_overall <- coxreg_prep_matching_death

    # Fit the Cox regression model
    overall.cox.nnm.pair <- coxph(Surv(death_time14, death14) ~ treatment + cluster(subclass), data=df_overall) 
    overall.cox.nnm.subject <- coxph(Surv(death_time14, death14) ~ treatment + cluster(person_id), data=df_overall) 
    overall.cox.nnm.cross <- coxph(Surv(death_time14, death14) ~ treatment + cluster(row_id), data=df_overall)
    
    # Get the variances
    overall.var.pair.nnm <- overall.cox.nnm.pair$var # variance1
    overall.var.subject.nnm <- overall.cox.nnm.subject$var # variance2
    overall.var.cross.nnm <- overall.cox.nnm.cross$var # variance3

    # Get pooled variance
    K1 = length(unique(df_overall$subclass)) # of unique pairs
    K2 = length(unique(df_overall$person_id)) # of unique patients
    K3 = length(unique(df_overall$row_id)) # of unique rows
    adjusted_variance = ((K1 / (K1 - 1)) * overall.var.pair.nnm) + ((K2 / (K2 - 1)) * overall.var.subject.nnm) - ((K3 / (K3 - 1)) * overall.var.cross.nnm)
    adjusted_SE = sqrt(adjusted_variance)

    ##### OUTPUT THE FINAL RESULTS TABLE
    
    # Return the relative risk from the first model (we can use any model)
    rr_conf <- exp(cbind(RR = coef(overall.cox.nnm.pair), confint(overall.cox.nnm.pair)))
    # rr_df <- as.data.frame(rr_conf) # Convert to data frame. 
    rr_df <- as.data.frame(rr_conf)["treatment",] # Convert to data frame and extract row for the coefficient of interest (treatment)
    names(rr_df) <- c("RR", "2_5", "97_5")

    # Assign the row index (which are variable names) as a column, so we can see the coefficient for each variable
    rr_df$variable <- rownames(rr_df)
    print(rr_df)

    # Return the coefficient and P-value
    coef_pval <- summary(overall.cox.nnm.pair)$coefficients
    # coef_pval <- as.data.frame(coef_pval)
    coef_pval <- as.data.frame(coef_pval)["treatment",]
    coef_pval$variable <- rownames(coef_pval)
    print(coef_pval)

    # Merge the tables together
    final <- coef_pval %>% inner_join(rr_df, by = "variable")

    #### Calculate the adjusted lower and upper limit; Then assign them as columns
    estimate <- final[1, "coef"]
    adjusted_LL <- exp(estimate - 1.96 * adjusted_SE)
    adjusted_UL <- exp(estimate + 1.96 * adjusted_SE)

    final$adjusted_LL <- adjusted_LL
    final$adjusted_UL <- adjusted_UL

    # Rename columns so we can output the table
    final <- final %>% rename("HR" = "exp(coef)","SE" = "se(coef)","Robust_SE" = "robust se", "P" = "Pr(>|z|)", "LL" = "2_5", "UL" = "97_5","HR_correct" = "RR") %>% select(-c("HR_correct"))

    # return
    return(data.frame(final))
    
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.51246f0a-8d87-4c6e-b97f-9bdbccc389df"),
    coxreg_prep_matching_deathhosp=Input(rid="ri.foundry.main.dataset.edd4f694-66db-4176-bde5-06d64e098f7b")
)
# Cox Regression for Sequential Trial (Paxlovid) (0d360c3d-7df7-4ac4-9286-35e10c14df7a): v3
coxreg_matching_deathhosp <- function(coxreg_prep_matching_deathhosp) {
    
    # This node is used to identify the variances for the treatment effect after clustering by pair, person, and row
    # This will enable us to get standard errors adjusted for replacement in the matching process
    
    library('survival')
    library('dplyr')
    
    df_overall <- coxreg_prep_matching_deathhosp

    # Fit the Cox regression model
    overall.cox.nnm.pair <- coxph(Surv(composite_death_hosp_time14, composite_death_hosp14) ~ treatment + cluster(subclass), data=df_overall) 
    overall.cox.nnm.subject <- coxph(Surv(composite_death_hosp_time14, composite_death_hosp14) ~ treatment + cluster(person_id), data=df_overall) 
    overall.cox.nnm.cross <- coxph(Surv(composite_death_hosp_time14, composite_death_hosp14) ~ treatment + cluster(row_id), data=df_overall)
    
    # Get the variances
    overall.var.pair.nnm <- overall.cox.nnm.pair$var # variance1
    overall.var.subject.nnm <- overall.cox.nnm.subject$var # variance2
    overall.var.cross.nnm <- overall.cox.nnm.cross$var # variance3

    # Get pooled variance
    K1 = length(unique(df_overall$subclass)) # of unique pairs
    K2 = length(unique(df_overall$person_id)) # of unique patients
    K3 = length(unique(df_overall$row_id)) # of unique rows
    adjusted_variance = ((K1 / (K1 - 1)) * overall.var.pair.nnm) + ((K2 / (K2 - 1)) * overall.var.subject.nnm) - ((K3 / (K3 - 1)) * overall.var.cross.nnm)
    adjusted_SE = sqrt(adjusted_variance)

    ##### OUTPUT THE FINAL RESULTS TABLE
    
    # Return the relative risk from the first model (we can use any model)
    rr_conf <- exp(cbind(RR = coef(overall.cox.nnm.pair), confint(overall.cox.nnm.pair)))
    # rr_df <- as.data.frame(rr_conf) # Convert to data frame. 
    rr_df <- as.data.frame(rr_conf)["treatment",] # Convert to data frame and extract row for the coefficient of interest (treatment)
    names(rr_df) <- c("RR", "2_5", "97_5")

    # Assign the row index (which are variable names) as a column, so we can see the coefficient for each variable
    rr_df$variable <- rownames(rr_df)
    print(rr_df)

    # Return the coefficient and P-value
    coef_pval <- summary(overall.cox.nnm.pair)$coefficients
    # coef_pval <- as.data.frame(coef_pval)
    coef_pval <- as.data.frame(coef_pval)["treatment",]
    coef_pval$variable <- rownames(coef_pval)
    print(coef_pval)

    # Merge the tables together
    final <- coef_pval %>% inner_join(rr_df, by = "variable")

    #### Calculate the adjusted lower and upper limit; Then assign them as columns
    estimate <- final[1, "coef"]
    adjusted_LL <- exp(estimate - 1.96 * adjusted_SE)
    adjusted_UL <- exp(estimate + 1.96 * adjusted_SE)

    final$adjusted_LL <- adjusted_LL
    final$adjusted_UL <- adjusted_UL

    # Rename columns so we can output the table
    final <- final %>% rename("HR" = "exp(coef)","SE" = "se(coef)","Robust_SE" = "robust se", "P" = "Pr(>|z|)", "LL" = "2_5", "UL" = "97_5","HR_correct" = "RR") %>% select(-c("HR_correct"))

    # return
    return(data.frame(final))
    
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6d3eb3b5-af0e-4831-9d10-c593c7158038"),
    propensity_model_all_weights_formatching_LC=Input(rid="ri.foundry.main.dataset.41cf61a7-c055-49df-aa97-408c434c6a9f")
)
nearest_neighbor_matching_LC <- function( propensity_model_all_weights_formatching_LC) {

    ## MATCH ON VARIANT
    library('dplyr')
    library('MatchIt')

    # Create copy of the dataset - then add a column with the logits (distance metric we will use)
    df <- propensity_model_all_weights_formatching_LC
    match_on_variant = TRUE

    # Perform the matching - 1 to many matching
    if (match_on_variant == TRUE) {
        variable_matching <- matchit(treatment ~ 1, 
        data = df, 
        method = "nearest",
        caliper = 0.1,
        std.caliper = TRUE,
        # ratio = n_controls/n_treated, ##### THIS IS FOR MATCHING A TREATED TO A SINGLE CONTROL
        ratio = 1, 
        # max.controls = 20, 
        # min.controls = 1, 
        replace = FALSE,
        distance = df$logit, 
        # link = "linear.logit",
        # exact = c('day'),
        exact = c('day','data_partner_id','variant','vaccination_count'), 
        antiexact = c('person_id')
        )
    } else {
        variable_matching <- matchit(treatment ~ 1, 
        data = df, 
        method = "nearest",
        caliper = 0.1,
        std.caliper = TRUE,
        # ratio = n_controls/n_treated, ##### THIS IS FOR MATCHING A TREATED TO A SINGLE CONTROL
        ratio = 1, 
        # max.controls = 20, 
        # min.controls = 1, 
        replace = FALSE,
        distance = df$logit, 
        # link = "linear.logit",
        # exact = c('day'),
        exact = c('day','data_partner_id','vaccination_count'), 
        antiexact = c('person_id')
        )
    }
    
    return(match.data(variable_matching))
    
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.7ed3e283-f6c6-43f0-baf6-ee29ea81c982"),
    propensity_model_all_weights_formatching=Input(rid="ri.foundry.main.dataset.90d544d7-c7c1-4267-ab20-a5ec7f49e9b9")
)
nearest_neighbor_matching_all <- function( propensity_model_all_weights_formatching) {

    ## MATCH ON VARIANT
    library('dplyr')
    library('MatchIt')

    # Create copy of the dataset - then add a column with the logits (distance metric we will use)
    df <- propensity_model_all_weights_formatching
    match_on_variant = TRUE

    # Perform the matching - 1 to many matching
    if (match_on_variant == TRUE) {
        variable_matching <- matchit(treatment ~ 1, 
        data = df, 
        method = "nearest",
        caliper = 0.1,
        std.caliper = TRUE,
        # ratio = n_controls/n_treated, ##### THIS IS FOR MATCHING A TREATED TO A SINGLE CONTROL
        ratio = 1, 
        # max.controls = 20, 
        # min.controls = 1, 
        replace = FALSE,
        distance = df$logit, 
        # link = "linear.logit",
        # exact = c('day'),
        exact = c('day','data_partner_id','variant','vaccination_count'), 
        antiexact = c('person_id')
        )
    } else {
        variable_matching <- matchit(treatment ~ 1, 
        data = df, 
        method = "nearest",
        caliper = 0.1,
        std.caliper = TRUE,
        # ratio = n_controls/n_treated, ##### THIS IS FOR MATCHING A TREATED TO A SINGLE CONTROL
        ratio = 1, 
        # max.controls = 20, 
        # min.controls = 1, 
        replace = FALSE,
        distance = df$logit, 
        # link = "linear.logit",
        # exact = c('day'),
        exact = c('day','data_partner_id','vaccination_count'), 
        antiexact = c('person_id')
        )
    }
    
    return(match.data(variable_matching))
    
}

