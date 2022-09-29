options(scipen=999)
options(max.print=99999)


setwd("C:/Users/James Niu/Dropbox/BA Group Project/BA Project Submission")


data = read.csv("probation-recidivism-4-8.csv")
head(data)


##########################################################################################
# Data Cleaning & Extraction, and Filling in Missing Values
##########################################################################################
# #Converting to factors/numerics/integers
# attach(data)
#     data$drug_hist = as.factor(drug_hist)
#     data$supervis = as.factor(supervis)
#     data$prob_length = as.numeric(prob_length)
#     data$age = as.integer(age)
#     data$arr_probation = as.factor(arr_probation)
# str(data)
# 
# #Running Multiple Imputation to fill in missing values
# install.packages("mice")
# library(mice)
# init = mice(data, maxit=0)
# meth = init$method
# predM = init$predictorMatrix
# 
# summary(data)
# 
# #Removing offence type and risk scores from imputation since there are no NAs here
# meth[c("offen_type")]=""
# meth[c("risk_score")]=""
# 
# #Running imputation 5 times and checking results
# set.seed(2)
# imputed = mice(data, method=meth, predictorMatrix=predM, m=5)
# imputed =  complete(imputed)
# sapply(imputed, function(x) sum(is.na(x))) ##no more NAs missing data
# remove(data)
# remove(init)
# remove(predM)
# remove(meth)
# write.csv(imputed,file="rec_data.csv")


rec_data <- read.csv("rec_data.csv")[,-1]
head(rec_data)


#Centering continuous vars & removing uncentered from a new dataframe,
#We now have average interpretation of continuous vars when they are zero
rec_data_cen <- rec_data
rec_data_cen$wage_hr_cen <- rec_data_cen$wage_hr - mean(rec_data_cen$wage_hr)
rec_data_cen$age_cen <- rec_data_cen$age - mean(rec_data_cen$age)
rec_data_cen$prob_length_cen <- rec_data_cen$prob_length - mean(rec_data_cen$prob_length)
rec_data_cen$risk_score_cen <- rec_data_cen$risk_score - mean(rec_data_cen$risk_score)
rec_data_cen$conv_num_cen <- rec_data_cen$conv_num - mean(rec_data_cen$conv_num)
rec_data_cen$wage_hr <- NULL
rec_data_cen$age <- NULL
rec_data_cen$risk_score <- NULL
rec_data_cen$prob_length <- NULL
rec_data_cen$conv_num <- NULL


#Adding all interaction terms
attach(rec_data_cen)
rec_data_interactions_cen <- model.matrix(arr_probation~.^2 , data = rec_data_cen)
detach(rec_data_cen)
rec_data_interactions_cen <- data.frame(rec_data_interactions_cen)
rec_data_interactions_cen$X.Intercept. <- NULL
#Adding centered polynomial terms for continuous vars
attach(rec_data_interactions_cen)
rec_data_interactions_cen$wage_hr_cen2 <- wage_hr_cen^2
rec_data_interactions_cen$age_cen_2 <- age_cen^2
rec_data_interactions_cen$conv_num_cen_2 <- conv_num_cen^2
rec_data_interactions_cen$prob_length_cen_2 <- prob_length_cen^2
rec_data_interactions_cen$risk_score_cen_2 <- risk_score_cen^2
detach(rec_data_interactions_cen)
attach(rec_data)
rec_data_interactions_cen <- cbind(rec_data_interactions_cen,arr_probation)
detach(rec_data)
##Final dataframe rec_data_interactions_cen has centered original, interactions, and polynomial vars
#If instead only adding centered polynomial terms without interactions
rec_data_polys_cen <- rec_data_cen
attach(rec_data_polys_cen)
rec_data_polys_cen$wage_hr_cen2 <- wage_hr_cen^2
rec_data_polys_cen$age_cen_2 <- age_cen^2
rec_data_polys_cen$conv_num_cen_2 <- conv_num_cen^2
rec_data_polys_cen$prob_length_cen_2 <- prob_length_cen^2
rec_data_polys_cen$risk_score_cen_2 <- risk_score_cen^2
detach(rec_data_polys_cen)
##Final dataframe rec_data_poly_cen has centered original, and polynomial vars


#Converting arr_probation to category(factor) for classification methods
rec_data_interactions_cen$arr_probation <- ifelse(rec_data_interactions_cen$arr_probation==1, "Y", "N") #Only run once!
rec_data_interactions_cen$arr_probation <- as.factor(rec_data_interactions_cen$arr_probation)
rec_data_polys_cen$arr_probation <- ifelse(rec_data_polys_cen$arr_probation==1, "Y", "N")
rec_data_polys_cen$arr_probation <- as.factor(rec_data_polys_cen$arr_probation)
rec_data_interactions_cen$drug_hist <- as.factor(rec_data_interactions_cen$drug_hist)
rec_data_polys_cen$drug_hist <- as.factor(rec_data_polys_cen$drug_hist)
rec_data_interactions_cen$supervis <- as.factor(rec_data_interactions_cen$supervis)
rec_data_polys_cen$supervis <- as.factor(rec_data_polys_cen$supervis)


#Split rec_data_interactions_cen into sample test data(25%), train data(50%), validation data(25%), e_train_data=train+valid
set.seed(1)
test_ind = sample(1:nrow(rec_data_interactions_cen),0.25*nrow(rec_data_interactions_cen)) 
test_data_interactions = rec_data_interactions_cen[test_ind,]
e_train_ind = -test_ind
e_train_data_interactions = rec_data_interactions_cen[e_train_ind,]
train_ind = sample(1:nrow(e_train_data_interactions),(2/3)*nrow(e_train_data_interactions))
train_data_interactions = e_train_data_interactions[train_ind,]
valid_ind = -train_ind
valid_data_interactions = e_train_data_interactions[-train_ind,]
#same for rec_data_polys_cen
set.seed(1)
test_ind = sample(1:nrow(rec_data_polys_cen),0.25*nrow(rec_data_polys_cen)) 
test_data_polys = rec_data_polys_cen[test_ind,]
e_train_ind = -test_ind
e_train_data_polys = rec_data_polys_cen[e_train_ind,]
train_ind = sample(1:nrow(e_train_data_polys),(2/3)*nrow(e_train_data_polys))
train_data_polys = e_train_data_polys[train_ind,]
valid_ind = -train_ind
valid_data_polys = e_train_data_polys[-train_ind,]


##########################################################################################
# Cross Validation, and Further Model Selection
##########################################################################################
#1)Log lasso regression w optimized lambda
#W/O Interaction Terms - See how coefficents change with lambda in lasso, and the most relevant regressors
install.packages("glmnet")
install.packages("plotmo")
library(glmnet)
library(plotmo)
x_polys = model.matrix(arr_probation~.,train_data_polys)[,-1]
y_polys = train_data_polys$arr_probation
grid = 10^(-4:4)
cv.out = cv.glmnet(x_polys,y_polys,type.measure="mse",alpha=1,lambda=grid,family="binomial",nfolds=10)
bestlam_polys = cv.out$lambda.min
bestlam_polys #bestlam_polys = 0.001
glm.lam_polys = glmnet(x_polys,y_polys,alpha=1,lambda=c(0.001,0.003, 0.005),family ="binomial")
plot_glmnet(glm.lam_polys,label=3,xvar="rlambda",grid.col ="lightgray")
##raceBlack in top three relevant predictors
#Fitting no interactions logistic model with best lamba
lasso.mod0 = glmnet(x_polys,y_polys,alpha=1,lambda=bestlam_polys,family="binomial")
summary(lasso.mod0)
lasso.mod0$beta #Shows the predictors chosen by lasso
#Log lasso error on validation data
x0 = model.matrix(arr_probation~.,valid_data_polys)[,-1]
y0 = valid_data_polys$arr_probation
pred = predict(lasso.mod0,x0,type="class")
table(y0,pred)
lasso_error = mean(pred!=y0)
lasso_error ##misclass error on valid data = 0.341
#W Interaction Terms - See how coefficents change with lambda in lasso, and the most relevant three regressors
#Cross-validation to optimized lambda
x = model.matrix(arr_probation~.,train_data_interactions)[,-1]
y = train_data_interactions$arr_probation
grid = 10^(-4:4)
cv.out = cv.glmnet(x,y,type.measure="mse",alpha=1,lambda=grid,family="binomial",nfolds=10)
bestlam = cv.out$lambda.min
bestlam #bestlam = 0.01
glm.lam = glmnet(x,y,alpha=1,lambda=c(0.01,0.02,0.03),family ="binomial")
plot_glmnet(glm.lam,label=3,xvar="rlambda",grid.col ="lightgray") 
##No more race variables in top three relevant predictors
#Fitting interactions logistic model with best lamba
lasso.mod = glmnet(x,y,alpha=1,lambda=bestlam,family="binomial")
summary(lasso.mod)
lasso.mod$beta #Shows the predictors chosen by lasso
#Log lasso error on validation data
x1 = model.matrix(arr_probation~.,valid_data_interactions)[,-1]
y1 = valid_data_interactions$arr_probation
pred = predict(lasso.mod,x1,type="class")
table(y1,pred)
lasso_error = mean(pred!=y1)
lasso_error ##misclass error on valid data = 0.331


#2)Decision tree w optimized prune
# install.packages("tree")
library(tree)
#note nature of the tree allows for interactions without stating it
tree.fit=tree(arr_probation~.,data=train_data_polys) 
summary(tree.fit)
plot(tree.fit)
text(tree.fit,pretty=0)
#Cross-validation to optimize prune parameter
set.seed(1)
cv.fit = cv.tree(tree.fit)
plot(cv.fit$size,cv.fit$dev,type="b")           #best prune parameter=2
prune.fit = prune.tree(tree.fit,best=2)
plot(prune.fit)
text(prune.fit,pretty=0)
#Tree error on validation data
tree.pred = predict(prune.fit,newdata=valid_data_polys,type="class")
table(valid_data_polys$arr_probation,tree.pred)
tree_error = mean(tree.pred!=valid_data_polys$arr_probation)
tree_error ##misclass error on valid data = 0.379


#3)LDA
# install.packages("MASS")
library(MASS)
#lda only works without interactions here
lda.fit = lda(arr_probation~.,data=train_data_polys) 
lda.fit
#LDA error on validation data
lda.pred=predict(lda.fit,valid_data_polys)
lda.class = lda.pred$class
table(valid_data_polys$arr_probation,lda.class)
lda_error = mean(lda.class!=valid_data_polys$arr_probation)
lda_error ##misclass error on valid data = 0.341


#4)KNN w optimized K nearest neighbours
# install.packages("class")
library(class)
#avoiding curse of dimensionality so only working on data with polynomials
#Scale the continuous variables in the data set (factors keep the same)
nums = unlist(lapply(train_data_polys,is.numeric))
train_polys_numeric=train_data_polys[,nums]
cols_to_keep <- intersect(colnames(train_data_polys),colnames(train_polys_numeric))
train_polys_remain=train_data_polys[ , !names(train_data_polys) %in% cols_to_keep] 
train_polys_scale=cbind(scale(train_polys_numeric),train_polys_remain)

#works as expected
nums = unlist(lapply(valid_data_polys,is.numeric))
valid_polys_numeric=valid_data_polys[,nums]
cols_to_keep <- intersect(colnames(valid_data_polys),colnames(valid_polys_numeric))
valid_polys_remain=valid_data_polys[ , !names(valid_data_polys) %in% cols_to_keep] 
valid_polys_scale=cbind(scale(valid_polys_numeric),valid_polys_remain)

#works as expected
#Converting data into model matrix
train_polys_scale_1=model.matrix(~.,train_polys_scale)
valid_polys_scale_1=model.matrix(~.,valid_polys_scale)
#summary(valid_polys_scale)
#str(train_polys_scale)
#Editing x and y of train, validatoin, and test data for use
x_train_polys_scale=train_polys_scale_1[,-34]
y_train_polys_scale=train_polys_scale_1[,34]
x_valid_polys_scale=valid_polys_scale_1[,-34]
y_valid_polys_scale=valid_polys_scale_1[,34]
#Train-Validation to optimized K=#nearest neighbours
# install.packages("caret")
library(caret)
predQuality = vector("numeric",10)
for (nn in 1:10){
  KNNpred = knn(x_train_polys_scale,x_valid_polys_scale,y_train_polys_scale, k = nn)
  predQuality[nn] = mean(KNNpred == y_valid_polys_scale)
}
best_k=which.max(predQuality)
print(best_k) #best k=8
KNNpred_validation = knn(x_train_polys_scale,x_valid_polys_scale,y_train_polys_scale,k = 8)
table(y_valid_polys_scale,KNNpred_validation)
mean(KNNpred_validation != y_valid_polys_scale) ##misclass error on valid data = 0.377
#Repeating KNN after removing race variables and
train_polys_scale_nonracial=train_polys_scale[,-18]
train_polys_scale_2=model.matrix(~.,train_polys_scale_nonracial)
valid_polys_scale_nonracial=valid_polys_scale[,-18]
valid_polys_scale_2=model.matrix(~.,valid_polys_scale_nonracial)
#Update the x and y of train, validation, test data
x_train_polys_scale=train_polys_scale_2[,-30]
y_train_polys_scale=train_polys_scale_2[,30]
x_valid_polys_scale=valid_polys_scale_2[,-30]
y_valid_polys_scale=valid_polys_scale_2[,30]
predQuality = vector("numeric",10)
for (nn in 1:10){
  KNNpred = knn(x_train_polys_scale,x_valid_polys_scale,y_train_polys_scale,k=nn)
  predQuality[nn] = mean(KNNpred==y_valid_polys_scale)
}
predQuality
best_k=which.max(predQuality)
best_k #best k=10
KNNpred_validation = knn(x_train_polys_scale,x_valid_polys_scale,y_train_polys_scale,k=10)
table(y_valid_polys_scale,KNNpred_validation)
mean(KNNpred_validation != y_valid_polys_scale) ##misclass error on valid data = 0.379


##########Retraining on e_train entire training data, and Optimizing Proability Threshold to Classify##########
#Retraining on entire training data
#Select Model Log lasso regression w interactions since lowest missor error on valid data=0.331, and also non racist/sexist
x_etrain = model.matrix(arr_probation~.,e_train_data_interactions)[,-1]
y_etrain = e_train_data_interactions$arr_probation
bestlam = 0.01
library(glmnet)
glm = glmnet(x_etrain,y_etrain,alpha=1,lambda=bestlam,family="binomial")
summary(glm)
glm$beta
#Assess on test data
x_test = model.matrix(arr_probation~.,test_data_interactions)[,-1]
y_test = test_data_interactions$arr_probation
pred_test = predict(glm,x_test,type="class")
table(y_test,pred_test)
test_error = mean(pred_test!=y_test)
test_error# test misclass rate is 0.341
#Select best threshold
pred_prob_test = predict(glm,x_test,type="response") ##asumptions can be changed from cost matrix excel
cost = c(c(0,31286),c(3200,26664.5))
pr = seq(from=0.05, to=0.75, by=0.01)
glm_cost = vector()
for (p in pr)
{
  decision = ifelse(pred_prob_test>p,'Y','N')
  classificationTable = table(y_test,decision)
  glm_cost = c(glm_cost,sum(classificationTable*cost))
}
glm_cost
min(glm_cost)
best_cost_ind=which.min(glm_cost)
pr[best_cost_ind]
##best threshold = 0.4 and minimum cost is 35,721,538
decision_best = ifelse(pred_prob_test>pr[best_cost_ind],'Y','N')
table(y_test,decision_best)
test_error = mean(decision_best!=y_test)
test_error# test misclass rate is 0.333

