# GAN for rare event outlier detection
This GAN makes of for the relative small number of outliers/targets found in some fraudulent interactions.
It facilitates the use of a very imbalanced dataset as it synthesize new outlier values which is possible to further train on.

An issue with this kind of GAN on tabular data is its tendency to get a mode collapse, essentially meaning the generator starts producing a similar set of output every time, which "fools" the discriminator.   

Credits: 
This GAN is heavily inspired by the book Machine Learning for Finance: Principles and practice for financial insiders by Jannes Klaas
