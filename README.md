# Big-data-projects

This repo contains three different exercises using `Spark` and `Java`:
- **es1:** This solves the problem of **triangle count** in a graph. In particular, this is done in two ways: firrstly with an approximation using *node coloring* and Spark Partitions.
- **es2:** This solves the problem of **triangle count** in a graph, using again both Spark Partitions and approximation algorithms. This exercise aims at computing the running time of the algorithm and showing that it can effectively handle inputs with 100M+ nodes in under 30 seconds.
- **es3:** This solves the problem of **count sketch** in a graph. The exercise aims at showing how the accuracy changes when varying the different variables of the input adn algorithm.

## Repository structure
The source code is inside the `src` folder, while the `result_tables` serves as a folder with the different results obtained throughout the runs. Finally, the `datasets` folder holds the datasets used, with the exception of the OrkutXM dataset, which is too large to be put in a repo, but can be easily be found online.
