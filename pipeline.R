

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6ace7dac-b666-4e91-bcf8-92b49288fe45"),
    coxreg_prep_matching_composite=Input(rid="ri.foundry.main.dataset.ff6099cf-f640-4e55-a82b-5d99a236d3f2")
)
# Logistic Regression for Outcome Analysis - Paxlovid (c3af488c-9754-4326-a36d-bed259d506ba): v0
OR_matching_14d <- function( coxreg_prep_matching_composite) {
    
    # This node is used to identify the variances for the treatment effect after clustering by pair, person, and row
    # This will enable us to get standard errors adjusted for replacement in the matching process
    
    library('survival')
    library('dplyr')
    library(survey)
    
    df_overall <- coxreg_prep_matching_composite

    # Set up the survey design; The cluster is person_id
    design_object <- svydesign(ids = ~subclass, weights = ~1, data = df_overall)

    # # Set up the quasipoisson model by choosing quasipoisson as the family. Treatment_indicator as the predictor. The target variable is 'outcome'
    model <- svyglm(composite14 ~ treatment, design = design_object, family=quasibinomial)
    # model <- svyglm(predictor_formula, design = design_object, family = quasibinomial(link = cloglog))

    # Print the model results
    print(summary(model))

    # Return the confidence intervals
    rr_conf <- exp(cbind(RR = coef(model), confint(model)))

    # Convert the rr_conf to a data frame (the second row) and rename the columns
    rr_df <- as.data.frame(rr_conf) #Confidence interval # rr_df <- as.data.frame(rr_conf)[2,] return specific row
    names(rr_df) <- c("RR", "2_5", "97_5")

    # Assign the row index (whcih are variable names) as a column, so we can see the coefficient for each variable
    rr_df$variable <- rownames(rr_df)

    # Return the coefficient and P-value
    coef_pval <- summary(model)$coefficients
    coef_pval <- as.data.frame(coef_pval)
    coef_pval$variable <- rownames(coef_pval)

    # Merge the tables together - coefficients and risk ratio (side by side, by the "variable" column)
    final <- coef_pval %>% inner_join(rr_df, by = "variable")

    # Rename the columns for SPARK - NEW = OLD
    final <- final %>% rename("SE" = "Std. Error", "t" = "t value", "P" = "Pr(>|t|)", "LL" = "2_5", "UL" = "97_5")

    return(data.frame(final))

}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8d1b3b82-9bcd-4768-babd-4486d6a3596f"),
    coxreg_prep_matching_composite_death_hosp=Input(rid="ri.foundry.main.dataset.1957ac02-7a23-44b0-8bb1-ecd8fff34c51")
)
# Logistic Regression for Outcome Analysis - Paxlovid (c3af488c-9754-4326-a36d-bed259d506ba): v0
OR_matching_14d_composite_death_hosp <- function( coxreg_prep_matching_composite_death_hosp) {
    
    # This node is used to identify the variances for the treatment effect after clustering by pair, person, and row
    # This will enable us to get standard errors adjusted for replacement in the matching process
    
    library('survival')
    library('dplyr')
    library(survey)
    
    df_overall <- coxreg_prep_matching_composite_death_hosp

    # Set up the survey design; The cluster is person_id
    design_object <- svydesign(ids = ~subclass, weights = ~1, data = df_overall)

    # # Set up the quasipoisson model by choosing quasipoisson as the family. Treatment_indicator as the predictor. The target variable is 'outcome'
    model <- svyglm(composite_death_hosp14 ~ treatment, design = design_object, family=quasibinomial)
    # model <- svyglm(predictor_formula, design = design_object, family = quasibinomial(link = cloglog))

    # Print the model results
    print(summary(model))

    # Return the confidence intervals
    rr_conf <- exp(cbind(RR = coef(model), confint(model)))

    # Convert the rr_conf to a data frame (the second row) and rename the columns
    rr_df <- as.data.frame(rr_conf) #Confidence interval # rr_df <- as.data.frame(rr_conf)[2,] return specific row
    names(rr_df) <- c("RR", "2_5", "97_5")

    # Assign the row index (whcih are variable names) as a column, so we can see the coefficient for each variable
    rr_df$variable <- rownames(rr_df)

    # Return the coefficient and P-value
    coef_pval <- summary(model)$coefficients
    coef_pval <- as.data.frame(coef_pval)
    coef_pval$variable <- rownames(coef_pval)

    # Merge the tables together - coefficients and risk ratio (side by side, by the "variable" column)
    final <- coef_pval %>% inner_join(rr_df, by = "variable")

    # Rename the columns for SPARK - NEW = OLD
    final <- final %>% rename("SE" = "Std. Error", "t" = "t value", "P" = "Pr(>|t|)", "LL" = "2_5", "UL" = "97_5")

    return(data.frame(final))

}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1b284bc0-d12a-43bc-a62c-7abda024ff96"),
    coxreg_prep_matching_death=Input(rid="ri.foundry.main.dataset.22c99e32-2f92-42c0-b2ec-412442b2c731")
)
# Logistic Regression for Outcome Analysis - Paxlovid (c3af488c-9754-4326-a36d-bed259d506ba): v0
OR_matching_14d_death <- function( coxreg_prep_matching_death) {
    
    # This node is used to identify the variances for the treatment effect after clustering by pair, person, and row
    # This will enable us to get standard errors adjusted for replacement in the matching process
    
    library('survival')
    library('dplyr')
    library(survey)
    
    df_overall <- coxreg_prep_matching_death

    # Set up the survey design; The cluster is person_id
    design_object <- svydesign(ids = ~subclass, weights = ~1, data = df_overall)

    # # Set up the quasipoisson model by choosing quasipoisson as the family. Treatment_indicator as the predictor. The target variable is 'outcome'
    model <- svyglm(death14 ~ treatment, design = design_object, family=quasibinomial(link = cloglog))
    # model <- svyglm(predictor_formula, design = design_object, family = quasibinomial(link = cloglog))

    # Print the model results
    print(summary(model))

    # Return the confidence intervals
    rr_conf <- exp(cbind(RR = coef(model), confint(model)))

    # Convert the rr_conf to a data frame (the second row) and rename the columns
    rr_df <- as.data.frame(rr_conf) #Confidence interval # rr_df <- as.data.frame(rr_conf)[2,] return specific row
    names(rr_df) <- c("RR", "2_5", "97_5")

    # Assign the row index (whcih are variable names) as a column, so we can see the coefficient for each variable
    rr_df$variable <- rownames(rr_df)

    # Return the coefficient and P-value
    coef_pval <- summary(model)$coefficients
    coef_pval <- as.data.frame(coef_pval)
    coef_pval$variable <- rownames(coef_pval)

    # Merge the tables together - coefficients and risk ratio (side by side, by the "variable" column)
    final <- coef_pval %>% inner_join(rr_df, by = "variable")

    # Rename the columns for SPARK - NEW = OLD
    final <- final %>% rename("SE" = "Std. Error", "t" = "t value", "P" = "Pr(>|t|)", "LL" = "2_5", "UL" = "97_5")

    return(data.frame(final))

}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.951b1d1e-e32a-4e3d-87aa-e555d6af9114"),
    coxreg_prep_matching_mod_composite=Input(rid="ri.foundry.main.dataset.edd4f694-66db-4176-bde5-06d64e098f7b")
)
# Logistic Regression for Outcome Analysis - Paxlovid (c3af488c-9754-4326-a36d-bed259d506ba): v0
OR_matching_14d_mod_composite <- function( coxreg_prep_matching_mod_composite) {
    
    # This node is used to identify the variances for the treatment effect after clustering by pair, person, and row
    # This will enable us to get standard errors adjusted for replacement in the matching process
    
    library('survival')
    library('dplyr')
    library(survey)
    
    df_overall <- coxreg_prep_matching_mod_composite

    # Set up the survey design; The cluster is person_id
    design_object <- svydesign(ids = ~subclass, weights = ~1, data = df_overall)

    # # Set up the quasipoisson model by choosing quasipoisson as the family. Treatment_indicator as the predictor. The target variable is 'outcome'
    model <- svyglm(mod_composite14 ~ treatment, design = design_object, family=quasibinomial)
    # model <- svyglm(predictor_formula, design = design_object, family = quasibinomial(link = cloglog))

    # Print the model results
    print(summary(model))

    # Return the confidence intervals
    rr_conf <- exp(cbind(RR = coef(model), confint(model)))

    # Convert the rr_conf to a data frame (the second row) and rename the columns
    rr_df <- as.data.frame(rr_conf) #Confidence interval # rr_df <- as.data.frame(rr_conf)[2,] return specific row
    names(rr_df) <- c("RR", "2_5", "97_5")

    # Assign the row index (whcih are variable names) as a column, so we can see the coefficient for each variable
    rr_df$variable <- rownames(rr_df)

    # Return the coefficient and P-value
    coef_pval <- summary(model)$coefficients
    coef_pval <- as.data.frame(coef_pval)
    coef_pval$variable <- rownames(coef_pval)

    # Merge the tables together - coefficients and risk ratio (side by side, by the "variable" column)
    final <- coef_pval %>% inner_join(rr_df, by = "variable")

    # Rename the columns for SPARK - NEW = OLD
    final <- final %>% rename("SE" = "Std. Error", "t" = "t value", "P" = "Pr(>|t|)", "LL" = "2_5", "UL" = "97_5")

    return(data.frame(final))

}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ca282b12-df63-4d80-a1d6-245d446596a0"),
    paxlovid_expanded_data_long_covid=Input(rid="ri.foundry.main.dataset.3d2b0e13-8f89-43c2-b72a-f1e6cad12d67")
)
Paxlovid_Long_COVID_DTSA <- function(paxlovid_expanded_data_long_covid) {

    # This node implements the DTSA
    using_odds <- FALSE
    dtsa_data <- paxlovid_expanded_data_long_covid
    library(dplyr)
    library(survey)

    # # Subset the dataset to only include the columns we need for the outcome model
    dtsa_data_subset <- dtsa_data

    # Set up predictor formula
    columns <- colnames(dtsa_data_subset)
    exclude <- c('person_id','subclass','row_id','outcome','time')
    
    # Create a new vector list of the columns excluding target, weight, and cluster variable
    predictors <- setdiff(columns, exclude) 

    # Make a formula from the predictor columns. If using discrete time, add "-1" to suppress the intercept
    continuous_time <- FALSE
    if (continuous_time == TRUE) {
        predictor_formula <- reformulate(predictors, "outcome")
        print(predictor_formula)
    } else {
        predictor_formula <- reformulate(c(predictors, -1), "outcome") 
        print(predictor_formula)
    }
    
    ############## 1. SET UP SURVEY OBJECTS AND FIT MODELS ############################################
    # Set up the survey design. The cluster is the subclass
    design_object1 <- svydesign(ids = ~subclass, weights = ~1, data = dtsa_data_subset)
    model1 <- svyglm(predictor_formula, design = design_object1, family = quasibinomial(link = cloglog)) 

    # Set up the survey design model 2: the cluster is person_id
    design_object2 <- svydesign(ids = ~person_id, weights = ~1, data = dtsa_data_subset)
    model2 <- svyglm(predictor_formula, design = design_object2, family = quasibinomial(link = cloglog)) 

    # Set up the survey design model 3: the cluster is row_id
    design_object3 <- svydesign(ids = ~row_id, weights = ~1, data = dtsa_data_subset)
    model3 <- svyglm(predictor_formula, design = design_object3, family = quasibinomial(link = cloglog)) 

    ############## 2. Extract Variances ##############
    variance_matrix1 <- as.data.frame(vcov(model1))
    print(variance_matrix1)
    variance1 <- variance_matrix1["treatment","treatment"] 

    variance_matrix2 <- as.data.frame(vcov(model2)) 
    variance2 <- variance_matrix2["treatment","treatment"] 

    variance_matrix3 <- as.data.frame(vcov(model3)) 
    variance3 <- variance_matrix3["treatment","treatment"]
    
    # Get the pooled variance
    K1 = length(unique(dtsa_data_subset$subclass)) # of unique pairs
    K2 = length(unique(dtsa_data_subset$person_id)) # of unique patients
    K3 = length(unique(dtsa_data_subset$row_id)) # of unique rows
    adjusted_variance = ((K1 / (K1 - 1)) * variance1) + ((K2 / (K2 - 1)) * variance2) - ((K3 / (K3 - 1)) * variance3)
    adjusted_SE = sqrt(adjusted_variance)

    ################# 3. OUTPUT THE FINAL RESULTS TABLE
    # Return the relative risk from the first model (we can use any model)
    rr_conf <- exp(cbind(RR = coef(model1), confint(model1)))
    # rr_df <- as.data.frame(rr_conf) # Convert to data frame. 
    rr_df <- as.data.frame(rr_conf)["treatment",] # Convert to data frame and extract row for the coefficient of interest (treated)
    names(rr_df) <- c("RR", "2_5", "97_5")

    # Assign the row index (which are variable names) as a column, so we can see the coefficient for each variable
    rr_df$variable <- rownames(rr_df)
    print(rr_df)

    # Return the coefficient and P-value
    coef_pval <- summary(model1)$coefficients
    # coef_pval <- as.data.frame(coef_pval)
    coef_pval <- as.data.frame(coef_pval)["treatment",]
    coef_pval$variable <- rownames(coef_pval)
    print(coef_pval)

    # Merge the tables together
    final <- coef_pval %>% inner_join(rr_df, by = "variable")

    #### Calculate the adjusted lower and upper limits (using the adjusted SE); Then assign them as columns in our results data frame; 
    estimate <- final[1, "Estimate"]
    adjusted_LL <- exp(estimate - 2.575829304 * adjusted_SE)
    adjusted_UL <- exp(estimate + 2.575829304 * adjusted_SE)

    final$adjusted_LL <- adjusted_LL
    final$adjusted_UL <- adjusted_UL

    # Rename the columns for SPARK
    final <- final %>% rename("SE" = "Std. Error", "t" = "t value", "P" = "Pr(>|t|)", "LL" = "2_5", "UL" = "97_5")

    return(final)
    
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.e54aab1a-0066-48da-81e7-f9914201fa39"),
    paxlovid_long_covid_coxreg_prep=Input(rid="ri.foundry.main.dataset.c1562765-12f3-4715-baa4-c02852eabc73")
)
# Cox Regression for Sequential Trial (Paxlovid) (0d360c3d-7df7-4ac4-9286-35e10c14df7a): v4
coxreg_long_covid <- function(paxlovid_long_covid_coxreg_prep) {
    
    # This node is used to identify the variances for the treatment effect after clustering by pair, person, and row
    # This will enable us to get standard errors adjusted for replacement in the matching process
    
    library('survival')
    library('dplyr')
    
    df_overall <- paxlovid_long_covid_coxreg_prep

    # Fit the Cox regression model
    overall.cox.nnm.pair <- coxph(Surv(long_covid_time,  outcome) ~ treatment + cluster(subclass), data=df_overall) 
    overall.cox.nnm.subject <- coxph(Surv(long_covid_time,  outcome) ~ treatment + cluster(person_id), data=df_overall) 
    overall.cox.nnm.cross <- coxph(Surv(long_covid_time,  outcome) ~ treatment + cluster(row_id), data=df_overall)
    
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
    Output(rid="ri.foundry.main.dataset.f09ea15f-655a-4eb2-828c-9a3520200d66"),
    coxreg_prep_matching_composite_death_hosp=Input(rid="ri.foundry.main.dataset.1957ac02-7a23-44b0-8bb1-ecd8fff34c51")
)
# Cox Regression for Sequential Trial (Paxlovid) (0d360c3d-7df7-4ac4-9286-35e10c14df7a): v3
coxreg_matching_composite_death_hosp <- function(coxreg_prep_matching_composite_death_hosp) {
    
    # This node is used to identify the variances for the treatment effect after clustering by pair, person, and row
    # This will enable us to get standard errors adjusted for replacement in the matching process
    
    library('survival')
    library('dplyr')
    
    df_overall <- coxreg_prep_matching_composite_death_hosp

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
    coxreg_prep_matching_mod_composite=Input(rid="ri.foundry.main.dataset.edd4f694-66db-4176-bde5-06d64e098f7b")
)
# Cox Regression for Sequential Trial (Paxlovid) (0d360c3d-7df7-4ac4-9286-35e10c14df7a): v2
coxreg_matching_modcomposite <- function(coxreg_prep_matching_mod_composite) {
    
    # This node is used to identify the variances for the treatment effect after clustering by pair, person, and row
    # This will enable us to get standard errors adjusted for replacement in the matching process
    
    library('survival')
    library('dplyr')
    
    df_overall <- coxreg_prep_matching_mod_composite

    # Fit the Cox regression model
    overall.cox.nnm.pair <- coxph(Surv(mod_composite_time14, mod_composite14) ~ treatment + cluster(subclass), data=df_overall) 
    overall.cox.nnm.subject <- coxph(Surv(mod_composite_time14, mod_composite14) ~ treatment + cluster(person_id), data=df_overall) 
    overall.cox.nnm.cross <- coxph(Surv(mod_composite_time14, mod_composite14) ~ treatment + cluster(row_id), data=df_overall)
    
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

