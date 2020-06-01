Final Project for CS505

COVID tracking system that uses a variety of database technologies to track disease transmission throughout the state of Kentucky through a web-based API. Uses a relational database for storing patient location, a graph database for directing patients to nearest available hospitals based on severity of illness, and a CEP for determining outbreaks per-zipcode using a simplified model (number of positive cases doubling between two consecutive 15 second windows). 

The graph is currently fully-connected, so graph traversal is fairly simplistic. This could easily be extended/modified for a graph that only contains edges between adjacent nodes. 

TODO:
* List instructions to build/run
* List API methods here
