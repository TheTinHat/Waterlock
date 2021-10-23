# Waterlock

Waterlock is a Python script meant for incrementally transferring data between three folder locations in two separate stages. It performs hash verification and persistently tracks data transfer progress using SQLite.


## Use Case & Features
The use-case Waterlock was designed for is moving files from one computer (i.e. your home server) to a intermediary drive (i.e. a portable hard drive), and then from the hard drive to another computer (i.e. an offsite backup server). 
- It will fill the intermediary drive with as many files as it can, aside from a user-configurable amount of reserve-space. 
- It performs blake2 checksums with every file copy, comparing it to the initial hash value stored in the SQLite database to ensure that data is not corrupted. 
- It uses a SQLite database to track what data has been moved. As a result, you can incrementally move data from one location to another with minimal user input. 
- Every time Waterlock is run on the source location, it will check for any files that have been recently modified (based on timestamp, not hash). Any modified files will have their hash & modification timestamps updated in the database, in addition to being marked as unmoved such that they are transferred again and updated. 
- Every time Waterlock is run on the source location, it will check for any files that were previously moved to the intermediary drive but did not reach the destination. If these files are no longer on the intermediary drive due to accidental deletion for instance, Waterlock will move those files to the intermediary drive again.


**Example Use Case**: I use Waterlock to transfer large files that are too large to transfer over the network to an offsite backup location at a relatives house. Each time I visit I run the script on my home server to load the external drive, then run it again on the offsite-backup server. 


## Usage
Change the settings at the top of the script, using *absolute file paths*. While relative paths may work, they are more error prone due to string formatting issues. Store the script on the intermediary drive itself and run it from there. It will automatically create `waterlock.db` and a `cargo` folder where the data will be stored. Note that after the final transfer to the destination, Waterlock will *not* delete data on the intermediary drive. 

```
python waterlock.py
```

If you are familiar with Python, you can also fully verify all the files on the middle or destination drives to ensure that the hashes match what is stored in the database. This is done using two additional class functions called `verify_middle()` and `verify_destination()`. The code to verify files on the destination would be as follows:

```
if __name__ == "__main__":
    wl = Waterlock( source_directory=source_directory, 
                    end_directory=end_direcotry, 
                    reserved_space=reserved_space
                    )
    wl.start()
    wl.verify_destination()
```



## Notes
It is named Waterlock after marine [locks](https://en.wikipedia.org/wiki/Lock_(water_navigation)) used to move ships through waterways of different water levels in multiple stages. 


*I am not responsible for any lost data. This was an evening coding project. **Use at your own discretion.***