# Predictive Analytics with Big Data Tecnologies
In order to use this application correctly you need to know first how to use the flags. For the very first time you run the application, you just need to execute:
> python main.py -model True -csv True

This flag `-model True` will create the directory "dataOutput" and save the prediction model there, so we will be able to reuse it later to save valuable time. After that, you will need to provide the input for the prediction, we suggest to use:
>Aircraft id: XY-LOL 
>Enter date (yyyy-mm-dd): 2012-03-07

Now you may have to wait for around (2-3) minutes while the databases are processed and the prediction model is trained. At the end, you will get an output that should look like something like this:
```
Test Error = 0.0540541 
Accuracy = 0.945946 
Recall = 1 
Maintenance IS required
------------------------------------------ 
Execution time: 
management(s): 39.22131609916687 
analysis(s): 58.366174936294556 
runtime(s): 3.2041420936584473 
total(s): 100.7916350364685 

Query: XY-LOL // 2012-03-07
```
As you see, the accuracy of our model is around a 95%, the recall is perfect and the prediction for our query input is `Maintenance IS required` . Now you can perform new queries saving a lot of time because the model has already been trained and the process over the data has also been done already. 
To try a new query, use:
> python main.py -reuse True

And now we suggest the input:
>Aircraft id: XY-YCV 
>Enter date (yyyy-mm-dd): 2015-06-01

And very soon you will get an output that should look like:
```
Maintenance IS NOT required 
------------------------------------------ 
Execution time: 
runtime(s): 10.650334119796753 

Query: XY-YCV // 2015-06-01
```
This time the prediction was `Maintenance IS NOT required` and the total execution time was less than 11 seconds. 

Finally, in case you may want to store the KPIs Matrix used to train the model, you can use the flag `-csv` when calling `main.py` and a .csv file will be saved in the "dataOutput" directory. For example you can call:

```
python main.py -model False -csv True
```
And while your prediction model won't be stored, the KPIs Matrix will be.
