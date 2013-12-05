=head1 LICENSE

Copyright [1999-2013] Wellcome Trust Sanger Institute and the EMBL-European Bioinformatics Institute

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
package Bio::EnsEMBL::DBLoader::RunnableDB::DownloadDatabase;

# Downloads the DB and performs a checksum of the files within

use strict;
use warnings;
use base qw/Bio::EnsEMBL::DBLoader::RunnableDB::Base/;

use Bio::EnsEMBL::Utils::Exception qw/throw warning/;
use Bio::EnsEMBL::Utils::Scalar qw/scope_guard/;
use Cwd;
use File::Path qw/mkpath rmtree/;
use File::Spec;
use IO::Uncompress::Gunzip qw($GunzipError);
use IO::File;

sub default_params {
  return {
    rsync => 1,
  };
}

sub fetch_input {
  my ($self) = @_;
  throw 'No work_directory has been given' unless $self->param('work_directory');
  throw 'No database has been given' unless $self->param('database');
  $self->SUPER::fetch_input();
  return;
}

sub run {
  my ($self) = @_;
  
  #Disconnect from the hive since we are going to be doing a lot of
  #FTP and file system intensive jobs
  $self->dbc()->disconnect_if_idle();
  $self->_create_local_dir(0);
  my $cwd = cwd();
  $self->cwd_local_dir();
  my $scope_guard = scope_guard(sub {
    chdir($cwd) or throw "Cannot cd back to '$cwd'";
  });

  #Only download from FTP if we are not reusing existing files.
  #Means files retreived from other mechanisms are still checksummed
  if($self->param('use_existing_files')) {
    my $database = $self->param('database');
    $self->cwd_local_dir($database);
  }
  else {
    $self->download();
  }
  
  $self->checksum();

  return;
}

sub download {
  my ($self) = @_;
  if($self->param('rsync')) {
    if($self->_rsync_url()) {
      $self->_rsync_download();
      return;
    }
    print STDERR "Switching to FTP as the submitted host does not support rsync\n" if $self->debug();
  }
  $self->_ftp_download();
  return;
}

sub _ftp_download {
  my ($self) = @_;
  my $ftp = $self->connect_ftp();
  my $directory = $self->base_ftp_path();
  $self->cwd_ftp_dir($directory);
  my $database = $self->param('database');
  $self->_create_local_dir(1, $database);
  $self->cwd_local_dir($database);
  $self->cwd_ftp_dir($database);
  my $ls = $self->ls_ftp_cwd();
  foreach my $file (@{$ls->{files}}) {
    $ftp->get($file) or throw "Cannot get the file $file from FTP ".$ftp->message();
  }
  $self->disconnect_ftp();
  return;
}

sub _rsync_download {
  my ($self) = @_;
  my $release = $self->param('release');
  my $base_rsync_url = $self->_rsync_url();
  my $database = $self->param('database');
  my $rsync_url = "${base_rsync_url}/release-${release}/mysql/${database}";
  my $verbose = ($self->debug()) ? '--verbose' : '--quiet';
  my $cmd = "rsync --recursive $verbose $rsync_url .";
  $self->_create_local_dir(0, $database);
  print STDERR "Running: $cmd\n" if $self->debug();
  system($cmd);
  my $rc = $? >> 8;
  if($rc != 0) {
    $self->throw("Encountered a problem whilst downloading files for ${database}. Rsync command was '$cmd'");
  }
  $self->cwd_local_dir($database);
  return;
}

sub checksum {
  my ($self) = @_;
  my $calculated_checksums = $self->_checksum_cwd();
  my $given_checksums = $self->_parse_checksums_file();

  my @failed_files;

  foreach my $file (sort keys %{$given_checksums}) {
    my $expected = $given_checksums->{$file};
    my $actual = $calculated_checksums->{$file} || '-';
    if($actual ne $expected) {
      push(@failed_files, $file);
      warning(sprintf(q{'%s' did not checksum. Expected %s but got %s}, $file, $expected, $actual));
    }
  }

  if(@failed_files) {
    my $list = join(q{,}, @failed_files);
    throw "The following files failed to checksum: [${list}]";
  }
  return;
}

sub _create_local_dir {
  my ($self, $remove_if_exists, @dirs) = @_;
  my $target = $self->local_dir(@dirs);
  if(-d $target) {
    if($remove_if_exists) {
      if($self->input_job()->retry_count() == 0) {
        throw "Cannot create the directory '${target}' as it already exists. Remove and rerun";
      }
      rmtree($target);
    }
  }
  mkpath($target);
  return;
}

sub _parse_checksums_file {
  my %checksums;

  my $fh;
  if(-f 'CHECKSUMS.gz') {
    $fh = IO::Uncompress::Gunzip->new('CHECKSUMS.gz') or throw "gunzip failed: $GunzipError\n";
  }
  elsif( -f 'CHECKSUMS') {
    $fh = IO::File->new('CHECKSUMS', 'r');
  }
  else {
    throw('No checksum data found @ '.cwd());
    return \%checksums;
  }

  while( my $line = $fh->getline()) {
    chomp($line);
    $line =~ /(\d+)\s+(\d+)\s+(.+)$/xms;
    my ($c1, $c2, $file) = ($1, $2, $3);
    $c1 = sprintf('%05d', $c1);
    my $checksum = "$c1 $c2";
    $checksums{$file} = $checksum;
  }

  $fh->close();

  return \%checksums;
}

sub _checksum_cwd {
  my ($self) = @_;
  my $dir = cwd();
  my %checksums;
  opendir(my $dh, $dir) or throw("Cannot open ${dir}: $!");
  my @files = readdir($dh);
  closedir($dh);
  foreach my $file (@files) {
    #Ignore if the file was CHECKSUMS, . (includes .hidden) or ..
    if($file !~ /CHECKSUMS/ && $file !~ /^\./xms && $file ne '\.\.' && -f $file) {
      $checksums{$file} = $self->_checksum_file($file);
    }
  }
  return \%checksums;
}

sub _checksum_file {
  my ($self, $file) = @_;
  if(! -f $file) {
    throw("Cannot find file $file");
  }
  my $checksum = `sum '$file'`;
  chomp($checksum);
  $checksum =~ /^(\d+)\s+(\d+)/;
  my ($c1, $c2) = ($1,$2);
  #We have to format to 5 digits
  $c1 = sprintf('%05d', $c1);
  my $check = "${c1} ${c2}";
  return $check;
}

sub _rsync_url {
  my ($self) = @_;
  if($self->param('ftp_host') eq 'ftp.ensembl.org') {
    return 'rsync://ftp.ensembl.org/ensembl/pub';
  }
  return;
}


1;