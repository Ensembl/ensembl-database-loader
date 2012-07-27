package Bio::EnsEMBL::DBLoader::RunnableDB::DownloadDatabase;

# Downloads the DB and performs a checksum of the files within

use strict;
use warnings;
use base qw/Bio::EnsEMBL::DBLoader::RunnableDB::Base/;

use Bio::EnsEMBL::Utils::Exception qw/throw warning/;
use Cwd;
use File::Path qw/mkpath rmtree/;
use File::Spec;
use IO::Uncompress::Gunzip qw($GunzipError);
use IO::File;

sub fetch_input {
  my ($self) = @_;
  throw 'No work_directory has been given' unless $self->param('work_directory');
  throw 'No database has been given' unless $self->param('database');
  return;
}

sub run {
  my ($self) = @_;

  $self->_create_local_dir(0);
  my $cwd = cwd();
  $self->cwd_local_dir();

  my $ftp = $self->connect_ftp();
  my $directory = $self->base_ftp_path();
  $self->cwd_ftp_dir($directory);
  $self->download($ftp);
  $self->disconnect_ftp();
  
  $self->checksum();

  chdir($cwd) or throw "Cannot cd back to '$cwd'";

  return;
}

sub download {
  my ($self, $ftp) = @_;
  my $database = $self->param('database');
  $self->_create_local_dir(1, $database);
  $self->cwd_local_dir($database);
  $self->cwd_ftp_dir($database);
  my $ls = $self->ls_ftp_cwd();
  foreach my $file (@{$ls->{files}}) {
    $ftp->get($file) or throw "Cannot get the file $file from FTP ".$ftp->message();
  }
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



1;