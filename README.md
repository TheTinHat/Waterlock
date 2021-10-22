# Waterlock

Waterlock is a Python script meant for securely transferring data between three folder locations in two separate stages. It performs hash verification and persistently tracks data transfer progress using SQLite.


## Use Case
The use-case Waterlock was designed for is moving files from one computer (i.e. your home server) to a intermediary drive (i.e. a portable hard drive), and then from the hard drive to another computer (i.e. an offsite backup server). It will fill the intermediary drive with as many files as it can, aside from a user-configurable amount of reserve-space. It performs blake2 checksums with every file copy to ensure that data is not corrupted. Finally, it uses a SQLite database to track what data has been moved. As a result, you can incrementally move data from one location to another with minimal user input. 

I use it to transfer large files that are too large to transfer over the network to an offsite backup location at a relatives house. Each time I visit I run the script on my home server to load the external drive, then run it again on the offsite-backup server. 


## Usage
Change the settings at the top of the script. Store the script on the intermediary drive itself and run it from there. It will automatically create `waterlock.db` and a `cargo` folder where the data will be stored. Note that after the final transfer to the destination, Waterlock will *not* delete data on the intermediary drive. 


It is named Waterlock after marine [locks](https://en.wikipedia.org/wiki/Lock_(water_navigation)) used to move ships through waterways of different water levels in multiple stages. 


## Note
I am not responsible for any lost data. This was an evening coding project. Use at your own discretion. 