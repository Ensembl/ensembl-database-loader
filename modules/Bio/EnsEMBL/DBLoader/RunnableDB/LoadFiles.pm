
=head1 LICENSE

Copyright [1999-2014] Wellcome Trust Sanger Institute and the EMBL-European Bioinformatics Institute

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

package Bio::EnsEMBL::DBLoader::RunnableDB::LoadFiles;

use strict;
use warnings;
use base
  qw/Bio::EnsEMBL::DBLoader::RunnableDB::Base Bio::EnsEMBL::DBLoader::RunnableDB::Database/;

use Bio::EnsEMBL::Utils::Exception qw/throw/;
use Bio::EnsEMBL::Utils::Scalar qw/wrap_array/;
use Cwd;
use IO::File;
use IO::Uncompress::Gunzip qw(gunzip $GunzipError);
use File::Spec;

sub fetch_input {
  my ($self) = @_;
  $self->SUPER::fetch_input();
  my $local_dir = $self->local_dir();
  my $sql_filename = sprintf( '%s.sql.gz', $self->database() );
  $self->param( 'sql_filename', $sql_filename );
  return;
}

sub run {
  my ($self) = @_;

  #Disconnect from the hive since we are going to be doing a lot of
  #DB & file system intensive operations
  if ( defined $self->dbc() ){
    $self->dbc()->disconnect_if_idle();
  }
  my $cwd = cwd();
  my $db  = $self->database();
#TODO Replace with pre_cleanup() which fires whenever we have a retry_count > 0
  if ( $self->_db_exists() ) {
    if ( $self->input_job()->retry_count() == 0 ) {
      throw
"Cannot continue. The database '$db' already exists and we are on our first attempt at loading";
    }
    print STDERR "Removing the database '$db' as it already exists\n"
      if $self->debug();
    $self->_remove_db();
  }
  $self->_create_db();
  $self->switch_db( $self->database() );
  chdir( $self->local_dir($db) ) or
    throw 'Cannot change to ' . $self->local_dir($db);

  $self->_load_sql();
  my %files = $self->_dump_files();
  foreach my $table ( sort keys %files ) {
    print STDERR "Processing $table\n" if $self->debug();
    $self->_load_data_file( $table, $files{$table} );
  }

  chdir($cwd) or throw 'Cannot change to ' . $cwd;

  return;
} ## end sub run

#### Private methods

sub _db_exists {
  my ($self) = @_;
  return ( $self->db_hash()->{ $self->database() } ) ? 1 : 0;
}

sub _remove_db {
  my ($self) = @_;
  my $db = $self->database();
  $self->target_dbc()->do("drop database `$db`");
  return;
}

sub _create_db {
  my ($self) = @_;
  my $db = $self->database();
  $self->target_dbc()->do("create database `$db`");
  return;
}

#Filters SQL into two files; 1 with tables & the other with views
sub _load_sql {
  my ($self) = @_;
  my $sql = $self->param('sql_filename');
  if ( !-f $sql ) {
    throw "Cannot find the expected SQL file $sql";
  }
  my $file = $self->_gunzip_file($sql);

  #Splitting view definitions into a separate file
  my $table_sql = $self->_filter_file(
    $file,
    sub {
      my ($line) = @_;
      return ( $line !~ /CREATE ALGORITHM/ ) ? 1 : 0;
    } );
  $self->run_mysql_cmd($table_sql);

  my $view_sql = $self->_filter_file(
    $file,
    sub {
      my ($line) = @_;
      return ( $line =~ /CREATE ALGORITHM/ ) ? 1 : 0;
    },
    sub {
      my ($line) = @_;
      $line =~
s/DEFINER=.+ \s+ SQL \s+ SECURITY \s+ DEFINER/SQL SECURITY INVOKER/xms;
      return $line;
    } );

  if ($view_sql) {
    $self->run_mysql_cmd($view_sql);
  }

  return;
} ## end sub _load_sql

#Loads everything from a file
sub _load_data_file {
  my ( $self, $table, $files ) = @_;

  my $is_view = $self->is_view($table);

  if ($is_view) {
    print STDERR "\tSkipping $table as it is a VIEW not a table\n"
      if $self->debug();
    return;
  }

  my $target_file = "${table}.txt";

  $self->_gunzip_file( $files, $target_file );

  print STDERR "\tLoading the table $table\n" if $self->debug();
  $self->_disable_indexes($table);

  my $force_escape = q{FIELDS ESCAPED BY '\\\\'};
  my $sql =
qq|LOAD DATA LOCAL INFILE '${target_file}' INTO TABLE `${table}` ${force_escape}|;
  $self->target_dbc()->do($sql);

  $self->_enable_indexes($table);
  $self->_analyze_table($table);
  print STDERR "\tFinished processing $table\n" if $self->debug();

  unlink($target_file);
  return;
} ## end sub _load_data_file

sub _gunzip_file {
  my ( $self, $files, $target ) = @_;
  $files = wrap_array($files);

  my $target_file = $target || $files->[0];
  $target_file =~ s/\.gz$//xms;
  unlink $target_file if -f $target_file;

  print STDERR sprintf( "Decompressing [%s] to %s\n",
                        join( q{, }, @{$files} ), $target )
    if $self->debug();
  foreach my $file ( @{$files} ) {
    gunzip $file => $target_file,
      Append => 1 or
      throw("gunzip failed from $file to $target : $GunzipError\n");
  }
  return $target_file;
}

sub _filter_file {
  my ( $self, $input_file, $filter, $alter ) = @_;
  my $output      = q{};
  my $found_lines = 0;
  open( my $input_fh, '<', $input_file ) or
    throw "Cannot open file $input_file: $!";
  while ( my $line = <$input_fh> ) {
    if ( $filter->($line) ) {
      $found_lines = 1;
      $line = $alter->($line) if defined $alter;
      $output .= $line;
    }
  }
  close $input_fh;
  return $output;
}

sub _dump_files {
  my ($self) = @_;
  my $dir = cwd();
  opendir( my $dh, $dir ) or throw("Cannot open ${dir}: $!");
  my @files = grep {
    my $f = $_;
    $f =~ /\.txt\.gz$/;
  } readdir($dh);
  closedir($dh);

# This is attempting to work with the old E! system where tables were split
# amongst multiple files. We need to strip out possible digits & the
# .txt.gz. All files are stored by their table name to aid loading
  my %file_hash;
  foreach my $file (@files) {
    #Tables can be alpha-numeric, underscores and even have a space
    $file =~ /([A-Za-z_0-9 ]+)(?:\.\d+)?\.txt\.gz/xms;
    my $table_name = $1;
    $file_hash{$table_name} = [] if !exists $file_hash{$table_name};
    push( @{ $file_hash{$table_name} }, $file );
  }

  foreach my $key ( keys %file_hash ) {
    my $data = $file_hash{$key};
    my @sorted_data = sort { $a cmp $b } @{$data};
    $file_hash{$key} = \@sorted_data;
  }

  return %file_hash;
} ## end sub _dump_files

sub _disable_indexes {
  my ( $self, $table ) = @_;
  $self->target_dbc()->do("alter table `${table}` disable keys");
  return;
}

sub _enable_indexes {
  my ( $self, $table ) = @_;
  $self->target_dbc()->do("alter table `${table}` enable keys");
  return;
}

sub _analyze_table {
  my ( $self, $table ) = @_;
  $self->target_dbc()->do("analyze table `${table}`");
  return;
}

1;
