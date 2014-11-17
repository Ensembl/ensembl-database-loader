# Ensembl Mirrors Pipeline

This is a re-implementation of the mirroring pipeline using the Hive infrastructure. This allows for a queue based scheduler to run the code as well as a local machine to load a database with automatic retry. For more information about Hive please consult http://www.ensembl.org/info/docs/eHive.html.

# Installing

This pipeline requires:

* A checkout of Ensembl core and hive (v68 minimum)
  * Please see http://www.ensembl.org/info/docs/eHive.html
  * The ENSEMBL_CVS_ROOT_DIR environment variable set to the checkout directory
* BioPerl 1.2.3
* A database for hive
  * MySQL
* A MySQL database server (5.1 minimum) for Ensembl
* Perl dependencies
  * Perl 5.8.9 (minimum)
  * Net::FTP
  * File::Spec
  * IO::File
  * IO::Uncompress::Gzip
  * DBI
  * DBD::MySQL
* Binary dependencies
  * mysql binary

# Running the Pipeline

## Selecting your databases & versions

By default the pipeline will load the API version of the core API you are using. If you want a full mirror than you can specify a different version using the `-release` command line argument.

If a subset of databases is required then specify each one using the `-databases` flag when initalising the pipeline 

## Working with Ensembl Genomes

You can download and host Ensembl Genomes databases by specifying the `-division` tag to flag which division we should look for the server in. The meaning of `-release` also changes to flag the *Ensembl Genomes release* i.e. release *10* not release 63. The functionality is untested but should work.

## Pipeline Parameters

|Name                 |Type                   |Multiple Supported |Description                                                                                  |Default              |Required|
|---------------------|-----------------------|-------------------|---------------------------------------------------------------------------------------------|---------------------|--------|
|ftp_host             |String                 |No                 |Host of the FTP server                                                                       |ftp.ensembl.org      |No      |
|ftp_port             |Integer                |No                 |Port of the FTP server                                                                       |21                   |No      |
|ftp_user             |String                 |No                 |User to connect as                                                                           |anonymous            |No      |
|ftp_pass             |String                 |No                 |Password to use                                                                              |                     |Yes     |
|rsync                |Boolean                |No                 |Use rsync over FTP                                                                           |0                    |No      |
|database             |String                 |Yes                |Databases to load. Leave blank to load all                                                   |                     |No      |
|mode                 |Enum(all,ensembl,mart) |No                 |Specify the databases to load when no databases were given. Mart mode targets just mart dbs  |all                  |No      |
|release              |Integer                |No                 |Ensembl Release to load                                                                      |Current API version  |No      |
|division             |String                 |No                 |Division to load. Ensembl Genomes only                                                       |                     |No      |
|work_directory       |String                 |No                 |Directory to download dumps to                                                               |                     |Yes     |
|use\_existing\_files |Boolean                |No                 |If true we will reuse files already found in `--work_directory`                              |0                    |No      |
|pipeline_name        |String                 |No                 |The pipeline name                                                                            |`mirror_all_version` |No      |
|target\_db\_host     |String                 |No                 |Host of the database to load DBs into                                                        |                     |Yes     |
|target\_db\_port     |Integer                |No                 |Port of the database to load DBs into                                                        |                     |Yes     |
|target\_db\_user     |String                 |No                 |User of the database to load DBs into. Needs FILE and CREATE permissions                     |                     |Yes     |
|target\_db\_pass     |String                 |No                 |Pass of the database to load DBs into                                                        |                     |No      |
|meadow_type          |String                 |No                 |Meadow to use. If you want to run locally use LOCAL                                          |LSF                  |No      |
|grant_users          |String                 |Yes                |Users to grant DB access to                                                                  |anonymous and ensro  |No      |

## Running the pipeline

### Environment Variables

#### ENSEMBL_CVS_ROOT_DIR

This should be set to the root of the directory for your ensembl checkouts. The directories must be named as they come out of CVS checkouts.

#### PERL5LIB

This should be set to the following:

```
PERL5LIB=$ENSEMBL_CVS_ROOT_DIR/ensembl-database-loader/modules:$PERL5LIB
PERL5LIB=$ENSEMBL_CVS_ROOT_DIR/ensembl/modules:$PERL5LIB
PERL5LIB=$ENSEMBL_CVS_ROOT_DIR/ensembl-hive/modules:$PERL5LIB
PERL5LIB=$ENSEMBL_CVS_ROOT_DIR/bioperl-live:$PERL5LIB
export PERL5LIB
```

#### PATH

Set to the following

`export PATH=$ENSEMBL_CVS_ROOT_DIR/ensembl-hive/scripts:$PATH`

#### Database and FTP Params

To make the guide easier we will export the settings for the target database. This is not a requirement but means you should be able to copy paste examples from this document out.

```
export ENSADMIN_PSW='mypass'

FTP_PASS='my@email.com'
DB_HOST='host.mysql'
DB_PORT=3306
DB_USER='user'
DB_PASS='password'

PIPELINE_HOST='my-host'
PIPELINE_PORT=3306
PIPELINE_USER='user'
```

You should notice the lack of a PIPELINE_PASS parameter or PIPELINE_DBNAME; the
first is handled by hive via the ENSADMIN_PSW export and the second is
automatically generated from your username and the pipeline name. If your user
name is wibble and we are loading Ensembl 68 databases then your database 
name will be *wibble_ensembl_mirror_68*.

## Example Setups 

### Current Checked-out API Release

```
usr@srv $ cd $ENSEMBL_CVS_ROOT_DIR/ensembl-database-loader
usr@srv $ init_pipeline.pl Bio::EnsEMBL::DBLoader::PipeConfig::LoadDBs_conf \
  -target_db_host $DB_HOST -target_db_port $DB_PORT -target_db_user $DB_USER -target_db_pass $DB_PASS  \
  -ftp_pass $FTP_PASS \
  -host $PIPELINE_HOST -pipeline_db -port=$PIPELINE_PORT -pipeline_db -user=$PIPELINE_USER \
  -work_directory /scratch/mywork
```

### Version Specific Databases

```
usr@srv $ cd $ENSEMBL_CVS_ROOT_DIR/ensembl-database-loader
usr@srv $ init_pipeline.pl Bio::EnsEMBL::DBLoader::PipeConfig::LoadDBs_conf \
  -target_db_host $DB_HOST -target_db_port $DB_PORT -target_db_user $DB_USER -target_db_pass $DB_PASS \
  -ftp_pass $FTP_PASS -release 64 \
  -host $PIPELINE_HOST -pipeline_db -port=$PIPELINE_PORT -pipeline_db -user=$PIPELINE_USER \
  -work_directory /scratch/mywork
```

### Ensembl Genomes Databases

```
usr@srv $ cd $ENSEMBL_CVS_ROOT_DIR/ensembl-database-loader
usr@srv $ init_pipeline.pl Bio::EnsEMBL::DBLoader::PipeConfig::LoadDBs_conf \
  -target_db_host $DB_HOST -target_db_port $DB_PORT -target_db_user $DB_USER -target_db_pass $DB_PASS \
  -ftp_pass $FTP_PASS \
  -division metazoa \
  -release 10 \
  -host $PIPELINE_HOST -pipeline_db -port=$PIPELINE_PORT -pipeline_db -user=$PIPELINE_USER \
  -work_directory /scratch/mywork
```

### Load a subset of Databases

```
usr@srv $ cd $ENSEMBL_CVS_ROOT_DIR/ensembl-database-loader
usr@srv $ init_pipeline.pl Bio::EnsEMBL::DBLoader::PipeConfig::LoadDBs_conf \
  -target_db_host $DB_HOST -target_db_port $DB_PORT -target_db_user $DB_USER -target_db_pass $DB_PASS \
  -ftp_pass $FTP_PASS \
  -databases saccharomyces_cerevisiae_core_66_4 -databases snp_mart_64 \
  -host $PIPELINE_HOST -pipeline_db -port=$PIPELINE_PORT -pipeline_db -user=$PIPELINE_USER \
  -work_directory /scratch/mywork
```

### Using files downloaded via another mechanism (assumes files and dirs are in work_directory)

```
usr@srv $ cd $ENSEMBL_CVS_ROOT_DIR/ensembl-database-loader
usr@srv $ init_pipeline.pl Bio::EnsEMBL::DBLoader::PipeConfig::LoadDBs_conf \
  -target_db_host $DB_HOST -target_db_port $DB_PORT -target_db_user $DB_USER -target_db_pass $DB_PASS \
  -use_existing_files 1 \
  -host $PIPELINE_HOST -pipeline_db -port=$PIPELINE_PORT -pipeline_db -user=$PIPELINE_USER \
  -work_directory /scratch/mywork
```

### Using rsync for the transport protocol

```
usr@srv $ cd $ENSEMBL_CVS_ROOT_DIR/ensembl-database-loader
usr@srv $ init_pipeline.pl Bio::EnsEMBL::DBLoader::PipeConfig::LoadDBs_conf \
  -target_db_host $DB_HOST -target_db_port $DB_PORT -target_db_user $DB_USER -target_db_pass $DB_PASS \
  -rsync 1 \
  -host $PIPELINE_HOST -pipeline_db -port=$PIPELINE_PORT -pipeline_db -user=$PIPELINE_USER \
  -work_directory /scratch/mywork
```

## Running the Pipeline

Hive will tell you to run a number of commands. The sync process is very important to run and means the setup will work. If you are running the code on a non-LSF system then specify local to run all commands locally. The code is quite memory light but processor intensive so please be aware of the ramifications of running a local pipeline.

