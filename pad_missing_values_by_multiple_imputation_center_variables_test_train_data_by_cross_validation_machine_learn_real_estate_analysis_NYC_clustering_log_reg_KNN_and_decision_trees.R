options(scipen=999)
options(max.print=99999)


setwd("C:/Users/James Niu/Dropbox/DA Group Project/Machine Learning")


# data = read.csv("data_raw_missing.csv")
# head(data)
# data$zip <- NULL
# 
# # # #########Data Cleaning & Extraction, and Filling in Missing Values###########
# # #Checking missing value cols and how many of them
# # sapply(data, function(x) sum(is.na(x)))
# #Converting to factors/numerics/integers
# str(data)
# ##All missing values are of type we want to predict
# 
# #Running Multiple Imputation to fill in missing values
# # install.packages("mice")
# library(mice)
# init = mice(data, maxit=0)
# meth = init$method
# predM = init$predictorMatrix
# #Removing no NAs column from imputation
# meth[c("price")]=""
# meth[c("race_div")] = ""
# meth[c("perc_asian")] = ""
# meth[c("perc_black")] = ""
# meth[c("perc_hispanic")] = ""
# meth[c("perc_white")] = ""
# meth[c("income_div")] = ""
# meth[c("unemp_rate")] = ""
# meth[c("educ_college")] = ""
# meth[c("poverty_rate")] = ""
# meth[c("pop_disable")] = ""
# meth[c("crime_sr")] = ""
# meth[c("tot_violate")] = ""
# meth[c("pop_den")] = ""
# meth[c("home_own")] = ""
# meth[c("kilometers_to_central_park")] = ""
# meth[c("kilometers_to_financial_district")] = ""
# 
# #Running imputation 5 times and checking results
# set.seed(1)
# imputed = mice(data,m=5,maxit=5,method='cart') #m is no of imputations. maxit is no of iterations
# imputed = complete(imputed)
# sapply(imputed, function(x) sum(is.na(x)))
# write.csv(imputed,file="h_data.csv",row.names=FALSE)
# remove(data)
# remove(init)
# remove(predM)
# remove(meth)


h_data = read.csv("h_data.csv")
str(h_data)


#Converting to age of house
h_data$age_property <- 2018-h_data$year_built
h_data$year_built <- NULL
#Making race_div as percentage probability of matching race in a randomly chosen pair
h_data$race_div <- h_data$race_div*100
head(h_data)


##Running regerssion for first impression
reg.fit=lm(price~.,data=h_data)
summary(reg.fit)
##need to normalize size for comparison it is heavily correlated with house_type
h_data$price_sqft <- h_data$price/h_data$size
h_data$price <- NULL
h_data$size <- NULL
head(h_data)
reg.fit=lm(price_sqft~.,data=h_data)
summary(reg.fit)
##coefficents make more sense now


#Split into sample test data(25%), train data(50%), validation data(25%), e_train_data=train+valid
set.seed(1)
test_ind = sample(1:nrow(h_data),0.25*nrow(h_data)) 
test_data = h_data[test_ind,]
e_train_ind = -test_ind
e_train_data = h_data[e_train_ind,]
train_ind = sample(1:nrow(e_train_data),(2/3)*nrow(e_train_data))
train_data = e_train_data[train_ind,]
valid_ind = -train_ind
valid_data = e_train_data[-train_ind,]


###########Applying Machine Learning Models & Getting Insights##########
#1)Linear lasso regression w optimized lambda
#Linear non-lasso regression
reg.fit=lm(price_sqft~.,data=train_data) #linear reg ##coefficients make sense
summary(reg.fit)
#Lasso to see how coefficents change with lambda, and choose most relevant predictors of house price
# install.packages("glmnet")
# install.packages("plotmo")
library(glmnet)
library(plotmo)
x = model.matrix(price_sqft~.,train_data)[,-1]
y = train_data$price_sqft
grid = 10^(-4:4)
cv.out = cv.glmnet(x,y,type.measure="mse",alpha=1,lambda=grid,family="gaussian",nfolds=10)
bestlam = cv.out$lambda.min
bestlam #bestlam = 1
glm.lam = glmnet(x,y,alpha=1,lambda=c(1,5,10),family ="gaussian")
plot_glmnet(glm.lam,label=6,xvar="rlambda",grid.col ="lightgray",main="Lasso Penalization")
##change labl=[x] for x most relevant predictors
#Fitting linear model with best lamba
lasso.mod = glmnet(x,y,alpha=1,lambda=bestlam,family="gaussian")
summary(lasso.mod)
lasso.mod$beta #Shows the predictors chosen by lasso
#Linear lasso error on validation data
x = model.matrix(price_sqft~.,valid_data)[,-1]
y = valid_data$price_sqft
pred = predict(lasso.mod,x)
mse = mean((pred-y)^2)
mse #mse=564240


#2)Decision tree w optimized prune
# install.packages("tree")
library(tree)
tree.fit=tree(price_sqft~.,data=train_data)
summary(tree.fit)
plot(tree.fit)
text(tree.fit,pretty=0)
#Cross-validation to optimize prune parameter
set.seed(1)
cv.fit = cv.tree(tree.fit)
plot(cv.fit$size,cv.fit$dev,type="b") #set prune parameter=6
prune.fit = prune.tree(tree.fit,best=6)
plot(prune.fit)
text(prune.fit,pretty=0)
#Tree error on validation data
tree.pred = predict(prune.fit,newdata=valid_data)
tree.valid=valid_data$price_sqft
mse = mean((tree.pred-tree.valid)^2)
mse #mse=530463


#3)KNN w optimized k=#nearest neighbours
# install.packages("caret")
library(caret)
normalize <- function(x) {
  num <- x - min(x)
  denom <- max(x) - min(x)
  return (num/denom)
}
temp1 = c(3,22) #index the unrequired y var, and factor vars can't be scaled
Data_norm <- as.data.frame(lapply(train_data[,-temp1], normalize)) #remove the unrequired vars and scale the rest
Data_norm$house_type <- train_data[,3] #put back factor vars
str(Data_norm)
# install.packages("psych")
library(psych)
#converting factor to dummys for numeric input
newcolumns = dummy.code(Data_norm$house_type)
Data_norm = cbind(Data_norm,newcolumns)
Data_norm$house_type = NULL
str(Data_norm)
#doing the KNN process
install.packages("FNN")
library(FNN)
mean_mse = c()
for (nn in 1:10){
  knn.fit = knn.reg(Data_norm,test=NULL,train_data[,22],k=nn)
  mean_mse[nn]=mean((knn.fit$pred-train_data[,22])^2)
}
best_k = which.min(mean_mse)
best_k #best k=9
#KNN error on validation data
Data_norm1 <- as.data.frame(lapply(valid_data[,-temp1], normalize)) #remove the unrequired vars and scale the rest
Data_norm1$house_type <- valid_data[,3] #put back factor vars
newcolumns = dummy.code(Data_norm1$house_type)
Data_norm1 = cbind(Data_norm1,newcolumns)
Data_norm1$house_type = NULL
str(Data_norm1)
knn.fit1 = knn.reg(Data_norm,test=Data_norm1,y=train_data[,22],k=best_k)
mse = mean((knn.fit1$pred-valid_data[,22])^2)
mse #mse=469699


##########Retraining on e_train entire training data, and assessing performance on test data###########
#Select KNN model w #nearest neighbours=9 since lowest mse on valid data=469699
#Preparting entire training data
Data_norm2 <- as.data.frame(lapply(e_train_data[,-temp1], normalize)) #remove the unrequired vars and scale the rest
Data_norm2$house_type <- e_train_data[,3] #put back factor vars
newcolumns = dummy.code(Data_norm2$house_type)
Data_norm2 = cbind(Data_norm2,newcolumns)
Data_norm2$house_type = NULL
str(Data_norm2)
#Preparing test data
Data_norm3 <- as.data.frame(lapply(test_data[,-temp1], normalize)) #remove the unrequired vars and scale the rest
Data_norm3$house_type <- test_data[,3] #put back factor vars
newcolumns = dummy.code(Data_norm3$house_type)
Data_norm3 = cbind(Data_norm3,newcolumns)
Data_norm3$house_type = NULL
str(Data_norm3)
#Assessing the test error
knn.fit2 = knn.reg(Data_norm2,test=Data_norm3,y=e_train_data[,22],k=best_k)
mse = mean((knn.fit2$pred-test_data[,22])^2)
mse #test mse=329181
plot(knn.fit2$pred,test_data[,22],xlab="prediction",ylab="actual",main="K-Nearest Neighbour to Predict Price Per Sqft")
abline(0,1,col="red")


#Try to Plot Linear Lasso Model
library(glmnet)
library(plotmo)
x = model.matrix(price_sqft~.,e_train_data)[,-1]
y = e_train_data$price_sqft
grid = 10^(-4:4)
cv.out = cv.glmnet(x,y,type.measure="mse",alpha=1,lambda=grid,family="gaussian",nfolds=10)
bestlam = cv.out$lambda.min
bestlam #bestlam = 0.1
#Fitting linear model with best lamba
lasso.mod = glmnet(x,y,alpha=1,lambda=bestlam,family="gaussian")
#Linear lasso error on validation data
x = model.matrix(price_sqft~.,test_data)[,-1]
y = test_data$price_sqft
pred = predict(lasso.mod,x)
plot(pred,test_data[,22],xlab="prediction",ylab="actual",main="Linear Lasso Regression to Predict Price Per Sqft")
abline(0,1,col="red")


#Try to Plot Tree Model
library(tree)
tree.fit=tree(price_sqft~.,data=e_train_data)
summary(tree.fit)
plot(tree.fit)
text(tree.fit,pretty=0)
#Cross-validation to optimize prune parameter
set.seed(1)
cv.fit = cv.tree(tree.fit)
plot(cv.fit$size,cv.fit$dev,type="b") #set prune parameter=6
prune.fit = prune.tree(tree.fit,best=6)
plot(prune.fit)
text(prune.fit,pretty=0)
#Tree error on validation data
tree.pred = predict(prune.fit,newdata=test_data)
tree.test=test_data$price_sqft
mse = mean((tree.pred-tree.test)^2)
plot(tree.pred,test_data[,22],xlab="prediction",ylab="actual",main="Classification Tree to Predict Price Per Sqft")
abline(0,1,col="red")


###########Clustering to Group Similar Zipcodes##########
m_data = read.csv("m_data.csv")
#Converting to age of house
m_data$age_property <- 2018-m_data$year_built
m_data$year_built <- NULL
m_data1 = data.frame(scale(m_data[,-1]))
head(m_data1)


#K-means clustering is to take into account pairewise relationships
rownames(m_data1) = make.names(m_data$zip, unique = TRUE)
# cdata2 <- cdata2[order(rownames(cdata2)),]
set.seed(1)
km.fit = kmeans(m_data1, centers=3, nstart=20) # for 3 clusters
plot(m_data1, col=km.fit$cluster, pch=20,lwd=7,main="K-Means Clustering")


#hierachy clustering to take into account all relationships on the table
set.seed(1)
hc.average=hclust(dist(m_data1), method="average")
par(cex=1.2) # change label size
plot(hc.average, main="Hierachical Clustering") # shows dendrogram

