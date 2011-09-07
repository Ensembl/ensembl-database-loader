package Bio::EnsEMBL::DBLoader::RunnableDB::ChecksumFiles;

use strict;
use warnings;
use base qw/Bio::EnsEMBL::DBLoader::RunnableDB::Base/;

use Bio::EnsEMBL::Utils::Exception qw/throw warning/;
use Cwd;
use File::Spec;
use IO::Uncompress::Gunzip qw($GunzipError);
use IO::File;

sub run {
  my ($self) = @_;

  my $cwd = cwd();
  $self->cwd_local_dir();

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

  chdir($cwd) or throw "Cannot cd back to '$cwd'";

  if(@failed_files) {
    my $list = join(q{,}, @failed_files);
    throw "The following files failed to checksum: [${list}]";
  }

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
      $checksums{$file} = $self->checksum_file($file);
    }
  }
  return \%checksums;
}

sub checksum_file {
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