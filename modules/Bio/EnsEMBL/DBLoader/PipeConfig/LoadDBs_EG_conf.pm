
=head1 LICENSE

Copyright [1999-2015] Wellcome Trust Sanger Institute and the EMBL-European Bioinformatics Institute
Copyright [2016-2017] EMBL-European Bioinformatics Institute

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

=cut

package Bio::EnsEMBL::DBLoader::PipeConfig::LoadDBs_EG_conf;

use strict;
use warnings;
use base qw/Bio::EnsEMBL::Hive::PipeConfig::HiveGeneric_conf/;

use Bio::EnsEMBL::ApiVersion;

sub default_options {
  my ($self) = @_;
  my $parent_options = $self->SUPER::default_options();
  my $options = {
    %{$parent_options},

    #FTP Settings
    ftp_host => 'ftp.ensemblgenomes.org',
    ftp_port => 21,
    ftp_user => 'anonymous',
    ftp_pass => '',

    #rsync flag
    rsync => 0,

    #Location of the MySQL dump, filesystem or ftp url.
    # E.g: /nfs/ensemblftp/PRIVATE/
    # E.g: rsync://ftp.ensembl.org/ensembl/pub
    rsync_url => '',

    #Required DBs; leave blank for all DBs
    databases => [],

#Mode; sets what we load. Defaults to all but ensembl & mart are available
    mode => 'all',

    #Set to the actual release or the current which
    release => 'current',

    #Only required when working with EnsemblGenomes servers
    division => '',

    #Work directory - location of the files where we can download to
    #work_directory => '',

#Set to true if you have already downloaded all the required files via another mechanism
    use_existing_files => 0,

    prerelease => 0,

    #Automatically set name
    pipeline_name => 'mirror_eg_' .
      $self->o('mode') . '_' . $self->o('release'),

    #Target DB
    target_db => { -host => $self->o('target_db_host'),
                   -port => $self->o('target_db_port'),
                   -user => $self->o('target_db_user'),
                   -pass => $self->o('target_db_pass') },

    # Meadow type; useful for forcing jobs into a particular meadow
    # by default this is LSF
    meadow_type => 'LSF',

    #Priority listing
    priority => { species => [], group => [qw/core variation/], },

    #grant users
    grant_users => [], };
  return $options;
} ## end sub default_options

#1 job - Figure out if we want all DBs or a subset
#5 jobs - Download all required databases & checksum
#Unlimited jobs - Prioritise into super, high & low
#2 jobs - Load DB SQL, Gunzip data & load tables

sub pipeline_analyses {
  my ($self) = @_;
  return [
    { -meadow_type => $self->o('meadow_type'),
      -logic_name  => 'find_dbs',
      -module => 'Bio::EnsEMBL::DBLoader::RunnableDB::DatabaseFactory',
      -meadow_type => 'LOCAL',
      -parameters  => {databases       => $self->o('databases'),
                       mode            => $self->o('mode'),
                       column_names    => ['database'],
                       fan_branch_code => 2,
                       randomize       => 1, },
      -input_ids => [   {} ],
      -flow_into => { 2 => [qw/download/] }, },

    { -meadow_type => $self->o('meadow_type'),
      -logic_name  => 'download',
      -module => 'Bio::EnsEMBL::DBLoader::RunnableDB::DownloadDatabase',
      -flow_into => { 1 => [qw/prioritise/] },
      -parameters           => { rsync => $self->o('rsync'),
                                 rsync_url => $self->o('rsync_url') },
      -hive_capacity        => 5,
      -failed_job_tolerance => 10, },

    { -meadow_type => 'LOCAL',
      -logic_name  => 'prioritise',
      -module      => 'Bio::EnsEMBL::DBLoader::RunnableDB::Prioritise',
      -parameters  => { priority => $self->o('priority') },
      -flow_into =>
        { 2 => ['load_files'], 3 => ['high_priority_load_files'] } },

    { -meadow_type   => $self->o('meadow_type'),
      -logic_name    => 'high_priority_load_files',
      -module        => 'Bio::EnsEMBL::DBLoader::RunnableDB::LoadFiles',
      -parameters    => { target_db => $self->o('target_db') },
      -hive_capacity => 8,
      -failed_job_tolerance => 100,
      -wait_for             => [qw/prioritise/],
      -can_be_empty         => 1,
      -flow_into =>
        { 1 => { 'grant' => { database => '#database#' } } }, },

    { -meadow_type   => $self->o('meadow_type'),
      -logic_name    => 'load_files',
      -module        => 'Bio::EnsEMBL::DBLoader::RunnableDB::LoadFiles',
      -parameters    => { target_db => $self->o('target_db') },
      -hive_capacity => 4,
      -wait_for      => [qw/prioritise high_priority_load_files/],
      -max_retry_count   => 1,
      -failed_job_tolerance => 50,
      -can_be_empty         => 1,
      -flow_into =>
        { 1 => { 'grant' => { database => '#database#' } } }, },

    { -meadow_type => 'LOCAL',
      -logic_name  => 'grant',
      -module      => 'Bio::EnsEMBL::DBLoader::RunnableDB::Grant',
      -parameters  => {
                  target_db                  => $self->o('target_db'),
                  user_submitted_grant_users => $self->o('grant_users'),
      },
      -max_retry_count  => 0,
      -can_be_empty => 1, },
                ####### NOTIFICATION
    {
      -meadow_type=> 'LOCAL',
      -logic_name => 'Notify',
      -module     => 'Bio::EnsEMBL::DBLoader::RunnableDB::EmailSummary',
      -parameters => {
          email   => $self->o('email'),
          subject => $self->o('pipeline_name').' has finished',
      },
      -wait_for   => ['grant','load_files','high_priority_load_files'],
    },];
} ## end sub pipeline_analyses

sub pipeline_wide_parameters {
  my ($self) = @_;
  return { %{ $self->SUPER::pipeline_wide_parameters() },
           release            => $self->o('release'),
           division           => $self->o('division'),
           ftp_host           => $self->o('ftp_host'),
           ftp_port           => $self->o('ftp_port'),
           ftp_user           => $self->o('ftp_user'),
           ftp_pass           => $self->o('ftp_pass'),
           work_directory     => $self->o('work_directory'),
           use_existing_files => $self->o('use_existing_files'),
           prerelease         => $self->o('prerelease'), };
}

sub resource_classes {
  my ($self) = @_;
  return {
      default => { 'LSF' => '-q production-rh7' },
      himem =>
        { 'LSF' => '-q production-rh7 -M 16000 -R "rusage[mem=16000]"' }
  };
}

1;
