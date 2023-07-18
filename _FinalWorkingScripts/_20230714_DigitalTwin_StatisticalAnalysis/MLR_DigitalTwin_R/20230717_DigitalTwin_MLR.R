#%%R

# Your R code here
# Load necessary libraries

#install.packages("lm.beta")
#install.packages("lmtest")
#install.packages("car")

library(lm.beta)
library(lmtest)
library(car)

# Read the data
building_data = `20230714_5T_weather_OccupancyBased`

#----Hierarchical Regression----

# Establish first model in hierarchical regression
mod1 <- lm(CarbonEmissions.kg.co2.h. ~ PeoplePerArea.p.m2., data = building_data)

# Summarize first model
summary(mod1)

# Establish second model in hierarchical regression
mod2 <- lm(CarbonEmissions.kg.co2.h. ~ PeoplePerArea.p.m2. + Temperature..degC. + Humidity.... + Wind.Speed..m.s. + Wind.Direction..deg. + Pressure..hPa. + LightingPowerIntensity.W.m2. + EquipmentPowerIntensity.W.m2. + VentilationRate..m3.s. + ACH, data = building_data)

# Summarize second model
summary(mod2)

# Compare models
anova(mod1, mod2)

# R Square change
changeMod <- summary(mod2)$r.squared - summary(mod1)$r.squared
changeMod

#----Diagnostics and Assumptions----

# Plot the second model
par(mfrow = c(2, 2))
plot(mod2)


#plot the second model using ggplot2

# load the necessary libraries
library(ggplot2)
library(gridExtra)

# calculate residuals and fitted values
residuals <- resid(mod2)
fitted <- fitted(mod2)
stdres <- rstandard(mod2)

# Residuals vs Fitted
p1 <- ggplot(data.frame(residuals = residuals, fitted = fitted), aes(x = fitted, y = residuals)) +
  geom_point(shape = 1, color = "#EE6A53") +
  geom_smooth(method = "lm", se = FALSE, color = "#656565") +
  theme_minimal() +
  labs(x = "Fitted values", y = "Residuals", title = "(A) Residuals vs Fitted") +
  theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank(), panel.background = element_rect(color = "black", size = 1))

# Normal Q-Q
p2 <- ggplot(data.frame(sample = stdres), aes(sample = sample)) +
  stat_qq(color = "#EE6A53") +
  stat_qq_line(color = "#656565") +
  theme_minimal() +
  labs(title = "(B) Normal Q-Q") +
  theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank(), panel.background = element_rect(color = "black", size = 1))

# Scale-Location
p3 <- ggplot(data.frame(sqrt_resid = sqrt(abs(stdres)), fitted = fitted), aes(x = fitted, y = sqrt_resid)) +
  geom_point(shape = 1, color = "#EE6A53") +
  geom_smooth(method = "lm", se = FALSE, color = "#656565") +
  theme_minimal() +
  labs(x = "Fitted values", y = "Sqrt(|standardized residuals|)", title = "(C) Scale-Location") +
  theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank(), panel.background = element_rect(color = "black", size = 1))

# Residuals vs Leverage
cooks_dist <- cooks.distance(mod2)
p4 <- ggplot(data.frame(leverage = hatvalues(mod2), stdres = stdres), aes(x = leverage, y = stdres)) +
  geom_point(aes(size = cooks_dist), shape = 1, color = "#EE6A53") +
  geom_hline(yintercept = 0, color = "#656565") +
  theme_minimal() +
  labs(x = "Leverage", y = "Standardized residuals", title = "(D) Residuals vs Leverage") +
  theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank(), panel.background = element_rect(color = "black", size = 1))

# arrange the plots into a 2x2 grid
grid.arrange(p1, p2, p3, p4, ncol = 2)





# Diagnostics
building_data$std.residuals <- rstandard(mod2)
building_data$Cooks.distance <- cooks.distance(mod2)
building_data$leverage <- hatvalues(mod2)

# Check discrepancy, leverage, and influence
which(building_data$std.residuals > 3.29 | building_data$std.residuals < -3.29)
which(building_data$leverage > 3*mean(building_data$leverage))
which(building_data$Cooks.distance > 1)

# Assumptions
building_data$residuals <- resid(mod2)

# Take the first 4999 residuals
residuals_sample <- building_data$residuals[1:4999]

# Perform the Shapiro-Wilk test
shapiro_test_result <- shapiro.test(residuals_sample)

# Print the result
print(shapiro_test_result)


# Test homoskedasticity of error
bptest(mod2, studentize = FALSE)

# Test independence of the error term
dwt(mod2)

# Test multicollinearity
vif(mod2) # Nothing larger than 10. 
1/vif(mod2) # Below .2 maybe, below .1 is an issue
mean(vif(mod2)) # Should not be substantially greater than 1

#----Final Model----
summary(mod1)
summary(mod2)

# Beta weights
lm.beta(mod2)
