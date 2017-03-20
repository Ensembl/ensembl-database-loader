
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

package Bio::EnsEMBL::DBLoader::RunnableDB::DatabaseFactory;

use strict;
use warnings;
use base
  qw/Bio::EnsEMBL::Hive::RunnableDB::JobFactory Bio::EnsEMBL::DBLoader::RunnableDB::Base/;

use Bio::EnsEMBL::Utils::Exception qw/throw/;
use File::Find;

my %allowed_modes = map { $_ => 1 } qw/mart ensembl all/;
my %filters = (
  mart => sub {
    my ( $self, $dir ) = @_;
    return $dir =~ /_mart_/ ? 1 : 0;
  },
  ensembl => sub {
    my ( $self, $dir ) = @_;
    return $dir !~ /_mart_/ ? 1 : 0;
  },
  all => sub {
    my ( $self, $dir ) = @_;
    my $databases = $self->param('databases');
    if ( $databases && ref($databases) eq 'ARRAY' && @{$databases} ) {
      my %lookup = map { $_ => 1 } @{$databases};
      if ( $lookup{$dir} ) {
        $self->param('hardcoded_db_hits')->{$dir} = 1;
        return 1;
      }
      return 0;
    }
    return 1;
  }, );

sub param_defaults {
  my ($self) = @_;
  return { mode               => 'all',
           hardcoded_db_hits  => {},
           use_existing_files => 0,
           prerelease         => 0 };
}

sub fetch_input {
  my ($self) = @_;

  Bio::EnsEMBL::DBLoader::RunnableDB::Base::fetch_input($self);

  my $mode = $self->param('mode') || q{};
  if ( !exists $allowed_modes{$mode} ) {
    my $join = join( q{, }, sort keys %allowed_modes );
    throw( sprintf(
                  q{Mode '%s' is not a supported mode. We support [%s]},
                  $mode, $join ) );
  }

  my $databases = $self->param('databases');
  if ($databases) {
    if ( !ref($databases) || ref($databases) ne 'ARRAY' ) {
      $databases = [$databases];
      $self->param( 'databases', $databases );
    }
#Just make sure we are asserting we have some kind of input (defaults to an array from the CFG)
    if ( @{$databases} ) {
      if ( $self->param('mode') ne 'all' ) {
        my $dbs = join( q{, }, @{$databases} );
        throw
"Cannot continue. You have requested to load '$dbs' but you are in mode '$mode'. Switch mode to 'all'";
      }
    }
  }

  my $dirs = $self->dirs();
  $self->param( 'inputlist', $dirs );

  #Check we loaded all DBs we asked to
  $self->assert_hardcoded_dbs();

  return;
} ## end sub fetch_input

sub dirs {
  my ($self) = @_;
  my $dirs;
  if ( $self->param('use_existing_files') or defined $self->param('rsync_url') ) {
    $dirs = $self->_local_dirs();
  }
  else {
    $dirs = $self->_ftp_dirs();
  }
  my $filter = $filters{ $self->param('mode') };
  my @ok_dirs;
  foreach my $dir ( @{$dirs} ) {
    printf STDERR "Processing '%s'\n", $dir if $self->debug();
    my $state = 'rejected';
    if ( $filter->( $self, $dir ) ) {
      $state = 'accepted';
      push( @ok_dirs, $dir );
    }
    printf STDERR "'%s' has been %s\n", $dir, $state if $self->debug();
  }
  return \@ok_dirs;
}

sub _local_dirs {
  my ($self) = @_;
  my $work_dir;
  if ($self->param('use_existing_files')){
    $work_dir=$self->local_dir();
  }
  elsif (defined $self->param('rsync_url')){
    $work_dir=$self->nfs_ftp_site_dir();
  }
  throw "$work_dir does not exist" if !-d $work_dir;
  opendir( my $dh, $work_dir ) or
    throw "Cannot open $work_dir for directory listing: $!";
  my @dirs = grep { $_ !~ /^\./ && -d "$work_dir/$_" } readdir($dh);
  close $dh;
  return \@dirs;
}

sub _ftp_dirs {
  my ($self)         = @_;
  my $ftp            = $self->connect_ftp();
  my $base_directory = $self->base_ftp_path();
  $self->cwd_ftp_dir($base_directory);
  my $ls = $self->ls_ftp_cwd();
  $self->disconnect_ftp();
  my $dirs = $ls->{dirs};
  return $dirs;
}

sub assert_hardcoded_dbs {
  my ($self) = @_;
  my $databases = $self->param('databases');
  my @missed;
  foreach my $db ( @{$databases} ) {
    if ( !$self->param('hardcoded_db_hits')->{$db} ) {
      push( @missed, $db );
    }
  }
  if (@missed) {
    my $j = join( q{, }, @missed );
    throw
"Could not find the following databases on the remote server [$j]";
  }
  return;
}

1;
