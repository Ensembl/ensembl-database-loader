package Bio::EnsEMBL::DBLoader::RunnableDB::Base;

use strict;
use warnings;
use base qw/Bio::EnsEMBL::Hive::Process/;

use Bio::EnsEMBL::Utils::Exception qw/throw/;
use Bio::EnsEMBL::Utils::Scalar qw/wrap_array/;
use File::Spec;
use Net::FTP;

#FTP ops

sub fetch_input {
  my ($self) = @_;
  my $work_directory = $self->param('work_directory');
  $work_directory = File::Spec->rel2abs($work_directory);
  $self->param('work_directory', $work_directory);
  return;
}

sub connect_ftp {
  my ($self) = @_;
  my ($server, $port, $user, $pass) = (map { $self->param("ftp_${_}") } qw/host port user pass/);
  my $debug = ($self->debug()) ? 2 : 0;
  my $ftp = Net::FTP->new( $server, Debug => $debug, Port => $port )
    or throw "Cannot connect to ${server}: $@";
  $ftp->login( $user, $pass ) or throw 'Cannot log in '. $ftp->message();
  $ftp->binary();
  $self->param('ftp', $ftp);
  return $ftp;
}

sub disconnect_ftp {
  my ($self) = @_;
  $self->param('ftp')->quit();
  return;
}

sub base_ftp_path {
  my ($self) = @_;
  my $release = $self->param('release');
  my $division = $self->param('division');
  my @path = ('', 'pub', "release-${release}");
  push(@path, $division) if $division;
  push(@path, 'mysql');
  return join(q{/},@path);
}

sub cwd_ftp_dir {
  my ($self, $dirs) = @_;
  my $ftp = $self->param('ftp');
  $dirs = wrap_array($dirs);
  foreach my $wd (@{$dirs}) {
    $ftp->cwd($wd) or throw "Cannot change working directory to ${wd}: ".$ftp->message();
  }
  return;
}

sub ls_ftp_cwd {
  my ($self) = @_;
  my $ftp = $self->param('ftp');
  my %files = (dirs => [], files => [] );
  my @ls = $ftp->dir() or throw "Cannot ls the current working FTP directory: ".$ftp->message();
  foreach my $file (@ls) {
    # Details come back like ls -l and grab first part which will look like
    # drwxr-xr-x
    my @split = split(/\s+/, $file, 9);
    my $type = substr($split[0], 0, 1);
    my $filename = $split[8];
    my $key = ($type eq 'd') ? 'dirs' : 'files';
    push(@{$files{$key}}, $filename);
  }
  return \%files;
}

##### Local File system ops

sub cwd_local_dir {
  my ($self, @dirs) = @_;
  my $dir = $self->local_dir(@dirs);
  chdir($dir) or throw "Cannot cd to '${dir}'";
  return;
}

sub local_dir {
  my ($self, @dirs) = @_;
  my $target = File::Spec->catdir($self->param('work_directory'), @dirs);
  return $target;
}

1;