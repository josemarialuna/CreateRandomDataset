# CreateRandomDataset

This package contains the code for generating Big Data random datasets in Spark to be used for clustering. Clusters are clearly defined and they follow a gaussian distribution. Datasets are generated taking as input the number of clusters, the number of features (columns), the number of instances on each cluster and the standard deviation of the distribution of the clusters. The instances could also include the class to which they belong.

Here there is two representations of two generated datasets:
![Dataset with 2 Features and 4 Clusters](https://github.com/josemarialuna/CreateRandomDataset/blob/master/2Features-4Clusters.PNG)

![Dataset with 3 Features and 5 Clusters](https://github.com/josemarialuna/CreateRandomDataset/blob/master/3Features-5Clusters.PNG)

These files were used to generate syntethic datasets used in [1].

Please, cite as: Luna-Romera, J.M., García-Gutiérrez, J., Martínez-Ballesteros, M. et al. Prog Artif Intell (2017). https://doi.org/10.1007/s13748-017-0135-3 (https://link.springer.com/article/10.1007%2Fs13748-017-0135-3)

## Getting Started
The package includes the following Scala files:
* RandomDataset: Scala Object that contains the methods that generate the synthetic datasets.
* MainCreateFile: Scala main class ready to generate the dataset.
* Dim20: Scala class used as an object with 20 attributes.
* Utils: Scala object that includes some helpful methods.

### Prerequisites

The package is ready to be used. You only have to download it and import it into your workspace. The main files include an example main that could be used.

## Running

The testing classes have been configured for being executed in a laptop. There are critical variables that must be configured before the execution:
* dimensions: Set the number of attributes (columns) of the dataset. The application is ready to generate from 2 until 20 features.
* clusters: Set the number of clusters.
* instances: Set the number of instances on each cluster.
* standDev: Set the standard deviation of the clusters.
* withClass: True or false that the instances include the class.


```
val conf = new SparkConf()
      .setAppName("Generate Random Dataset")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    var dimensions = 3
    var clusters = 5
    var instances = 100
    var standDev = 0.05f
    var withClass = false

    RandomDataset.createFile(sc, dimensions, clusters, instances, standDev, "", withClass)
```


## Results

By default, results are saved in the same a folder than the dataset. Results are named "C[Clusters]-D[Features]-I[Instances]-DateTime" that contains part-00000 file. 

## Contributors

* José María Luna-Romera - (main contributor and maintainer).
* José C. Riquelme Santos

## References

[1] Luna-Romera, J.M., García-Gutiérrez, J., Martínez-Ballesteros, M. et al. Prog Artif Intell (2017). https://doi.org/10.1007/s13748-017-0135-3

