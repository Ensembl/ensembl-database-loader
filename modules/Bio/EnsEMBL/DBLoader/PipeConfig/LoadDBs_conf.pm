package Bio::EnsEMBL::DBLoader::PipeConfig::LoadDBs_conf;

use strict;
use warnings;
use base qw/Bio::EnsEMBL::Hive::PipeConfig::HiveGeneric_conf/;

use Bio::EnsEMBL::ApiVersion;

sub default_options {
  my ($self) = @_;
  my $parent_options = $self->SUPER::default_options();
  return {
    %{$parent_options},

    #FTP Settings
    ftp_host => 'ftp.ensembl.org',
    ftp_port => 21,
    ftp_user => 'anonymous',
#    ftp_pass => ''

    #Required DBs; leave blank for all DBs
    required_dbs_file => '',

    #Set to the actual release or the current which
    release => software_version(),

    #Only required when working with EnsemblGenomes servers
    division => '',

    #Work directory - location of the files where we can download to
    #work_directory => '',

    #Automatically set name
    pipeline_name => 'mirror_ensembl_'.$self->o('release'),

    #Target DB
    target_db => {
      -host => $self->o('target_db_host'),
      -port => $self->o('target_db_port'),
      -user => $self->o('target_db_user'),
      -pass => $self->o('target_db_pass')
    },

    #Setting the Hive DB which we will use SQLite for
    pipeline_db => {
      -driver => 'sqlite',
      -dbname => $self->o('pipeline_name'),
      -host   => '',
      -port   => '',
      -user   => '',
      -pass   => '',
    },

    #Priority listing
    priority => {
      species => [qw/homo_sapiens mus_musculus danio_rerio/],
      group => [qw/core variation/],
    }
  };
}

sub pipeline_wide_parameters {
  my ($self) = @_;
  return {
    %{$self->SUPER::pipeline_wide_parameters()},
    'work_directory' => $self->o('work_directory'),
  };
}

sub pipeline_create_commands {
  my ($self) = @_;
  my $parent_cmds = $self->SUPER::pipeline_create_commands();
  return [
    @{$parent_cmds},
    'mkdir -p '.$self->o('work_directory'),
  ];
}

#1 job - Figure out if we want all DBs or a subset
#1-3 jobs - Download all required directories (database and datafiles)
#10 jobs - Checksum all files
#1 job - Sub-divide DBs into high priority & low priority by DB type & species
#2 jobs - Load DB SQL, Gunzip data & load tables

sub pipeline_analyses {
  my ($self) = @_;
  return [
    {
      -logic_name => 'find_dbs',
      -module => 'Bio::EnsEMBL::DBLoader::RunnableDB::FindDbs',
      -parameters => {
        required_dbs_file => $self->o('required_dbs_file'),
        release           => $self->o('release'),
        division          => $self->o('division'),
        ftp_host          => $self->o('ftp_host'),
        ftp_port          => $self->o('ftp_port'),
        ftp_user          => $self->o('ftp_user'),
        ftp_pass          => $self->o('ftp_pass'),
      },
      -input_ids => [{}],
      -flow_into => { 1 => [qw/download_files/] },
    },

    {
      -logic_name => 'download_files',
      -module => 'Bio::EnsEMBL::DBLoader::RunnableDB::DownloadFiles',
      -parameters => {
        ftp_host  => $self->o('ftp_host'),
        ftp_port  => $self->o('ftp_port'),
        ftp_user  => $self->o('ftp_user'),
        ftp_pass  => $self->o('ftp_pass'),
      },
      -flow_into => { 2 => [qw/checksum_files/] },
      -hive_capacity => 1,
      -failed_job_tolerance => 10,
    },

    {
      -logic_name => 'checksum_files',
      -module => 'Bio::EnsEMBL::DBLoader::RunnableDB::ChecksumFiles',
      -parameters => {},
      -hive_capacity => 10,
      -flow_into => { 1 => [qw/prioritise/] },
      -failed_job_tolerance => 10,
    },

    {
      -logic_name => 'prioritise',
      -module => 'Bio::EnsEMBL::DBLoader::RunnableDB::Prioritise',
      -parameters => { priority => $self->o('priority') },
      -flow_into => { 2 => [qw/high_priority_load_files/], 3 => [qw/load_files/] },
    },

    {
      -logic_name => 'high_priority_load_files',
      -module => 'Bio::EnsEMBL::DBLoader::RunnableDB::LoadFiles',
      -parameters => { target_db => $self->o('target_db') },
      -hive_capacity => 1,
      -failed_job_tolerance => 100,
      -can_be_empty => 1,
      -flow_into => { 2 => [qw/grant/] }
    },

    {
      -logic_name => 'load_files',
      -module => 'Bio::EnsEMBL::DBLoader::RunnableDB::LoadFiles',
      -parameters => { target_db => $self->o('target_db') },
      -hive_capacity => 1,
      -wait_for => [qw/prioritise high_priority_load_files/],
      -retry_count => 1,
      -failed_job_tolerance => 50,
      -can_be_empty => 1,
      -flow_into => { 1 => [qw/grant/] }
    },
    
    {
      -logic_name => 'grant',
      -module => 'Bio::EnsEMBL::DBLoader::RunnableDB::Grant',
      -parameters => { target_db => $self->o('target_db') },
      -hive_capacity => 1,
      -wait_for => [qw/prioritise high_priority_load_files/],
      -retry_count => 1,
      -failed_job_tolerance => 50,
      -can_be_empty => 1,
    },
  ];
}

1;