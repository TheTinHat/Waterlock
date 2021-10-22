# Waterlock

Waterlock is a Python script meant for transferring data between three folder locations in two separate stages. 


## Use Case
The use-case Waterlock was designed for is moving files from one computer (i.e. your home server) to a intermediary drive (i.e. a portable hard drive), and then from the hard drive to another computer (i.e. an offsite backup server). It will fill the intermediary drive with as many files as it can, aside from a user-configurable amount of reserve-space. It performs blake2 checksums with every file copy to ensure that data is not corrupted. Finally, it uses a SQLite database to track what data has been moved. As a result, you can incrementally move data from one location to another with minimal user input. 


## Usage
Change the settings at the top of the script. Store the script on the intermediary drive itself and run it from there. It will automatically create `waterlock.db` and a `cargo` folder where the data will be stored.


It is named Waterlock after marine [locks](https://en.wikipedia.org/wiki/Lock_(water_navigation) used to move ships through waterways of different water levels in multiple stages. 