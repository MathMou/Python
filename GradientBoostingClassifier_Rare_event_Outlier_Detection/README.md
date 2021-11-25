# GradientBoostingClassifier
Template for setting up a rudimentary GradientBoostingClassifier for imbalanced dataset able to detect outliers on with a high percentage

# basic_parameter_tuning.py: 
Used to finetune parameters in isolation for GradientBoostingClassifier.
In high dimensional data with a large quantity of datapoints, use the finetuning isolated with other parameters being default and test the combinations in the end. 

# gradientboostingclassifier_spark-impala_output.py:
Main run file to start creating a model on train_test data + a output on new data without the possibility of testing accuracy.
