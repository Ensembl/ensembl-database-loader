package Bio::EnsEMBL::DBLoader::RunnableDB::DownloadFiles;

use strict;
use warnings;
use base qw/Bio::EnsEMBL::DBLoader::RunnableDB::Base/;

use Bio::EnsEMBL::Utils::Exception qw/throw/;
use Cwd;
use File::Path qw/mkpath/;
use File::Spec;

sub fetch_input {
  my ($self) = @_;
  throw 'No directory has been given' unless $self->param('directory');
  throw 'No work_directory has been given' unless $self->param('work_directory');
  return;
}

sub run {
  my ($self) = @_;

  $self->_create_local_dir();
  my $cwd = cwd();
  $self->cwd_local_dir();

  my $ftp = $self->connect_ftp();
  my $directory = $self->param('directory');
  $self->cwd_ftp_dir($directory);
  $self->recurse();
  $self->disconnect_ftp();

  chdir($cwd) or throw "Cannot cd back to '$cwd'";

  return;
}

sub recurse {
  my ($self, @dirs) = @_;
  
  my $cwd;
  if(@dirs) {
    $cwd = cwd();
    $self->_create_local_dir(@dirs);
    $self->cwd_local_dir(@dirs);
    
    my $next_dir = $dirs[-1];
    $self->cwd_ftp_dir($next_dir);
  }
  
  my $ls = $self->ls_ftp_cwd();
  foreach my $file (@{$ls->{files}}) {
    $ftp->get($file) or throw "Cannot get the file $file from FTP ".$ftp->message();
  }
  foreach my $dir (@{$ls->{dirs}}) {
    $self->recurse(@dirs, $dir);
  }
  
  if($cwd) {
    $self->cwd_ftp_dir('..');
    chdir($cwd) or throw "Cannot cd back to '$cwd'";
  }
  
  return;
}

sub _create_local_dir {
  my ($self, @dirs) = @_;
  my $target = $self->local_dir(@dirs);
  if(-d $target) {
    throw "Cannot create the directory '${target}' as it already exists. Remove and rerun";
  }
  mkpath($target);
  return;
}

sub write_output {
  my ($self) = @_;
  return;
}

1;