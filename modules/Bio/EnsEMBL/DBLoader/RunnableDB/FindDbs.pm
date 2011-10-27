package Bio::EnsEMBL::DBLoader::RunnableDB::FindDbs;

use strict;
use warnings;
use base qw/Bio::EnsEMBL::DBLoader::RunnableDB::Base/;

use Bio::EnsEMBL::ApiVersion;
use Bio::EnsEMBL::Utils::Exception qw/throw/;
use Bio::EnsEMBL::Utils::IO qw/slurp_to_array/;

sub param_defaults {
  my ($self) = @_;
  return {
    required_dbs_file => '',
    release => software_version(),
    division => '',
    mysql => 1,
    data_files => 1
  };
}

sub fetch_input {
  my ($self) = @_;
  
  my $file = $self->param('required_dbs_file');
  my %required_dbs;
  if(-f $file) {
    %required_dbs = map { $_ => 1 } @{slurp_to_array($file, 1)};
  } 
  $self->param('required_dbs_hash', \%required_dbs);

  return;
}

sub run {
  my ($self) = @_;

  $self->connect_ftp();
  my $base_path = $self->base_ftp_path();
  $self->cwd_ftp_dir($base_path);
  my $base_dir_ls = $self->ls_ftp_cwd();
  my %dirs_hash = map { $_ => 1 } @{$base_dir_ls->{dirs}};

  my @targets;

  if($self->param('mysql')) {
    if(exists $dirs_hash{mysql}) {
      foreach my $target ($self->_cwd_and_filter($base_path, 'mysql')) {
        push(@targets, { loc => $target, checksum => 1 });
      }
    }
    else {
      my $string_path = join(q{/}, @{$base_path});
      throw sprintf('No mysql directory found at %s', $string_path);
    }
  }

  if($self->param('data_files')) {
    if(exists $dirs_hash{data_files}) {
      $self->cwd_ftp_dir($base_path);
      foreach my $target ($self->_cwd_and_filter($base_path, 'data_files')) {
        push(@targets, { loc => $target, checksum => 0 });
      }
    }
  }

  $self->param('targets', \@targets);

  $self->disconnect_ftp();

  return;
}

sub _cwd_and_filter {
  my ($self, $base_path, $target_dir) = @_;
  $self->cwd_ftp_dir($target_dir);

  my @targets;

  my $required_dbs = $self->param('required_dbs_hash');
  my $process_required_dbs = (%{$required_dbs}) ? 1 : 0;
  my $listings = $self->ls_ftp_cwd()->{dirs};

  foreach my $sub_dir (@{$listings}) {
    if($process_required_dbs) {
      if(! exists $required_dbs->{$sub_dir}) {
        print STDERR sprintf("Skipping %s as it was not in the required DBs list\n", $sub_dir) if $self->debug();
        next;
      }
    }
    push(@targets, [@{$base_path}, $target_dir, $sub_dir]);
  }

  return @targets;
}

sub write_output {
  my ($self) = @_;
  my $targets = $self->param('targets');
  foreach my $t (@{$targets}) {
    my $hash = {
      directory => $t->{loc},
      checksum => $t->{checksum}
    };
    $self->dataflow_output_id($hash, 1);
  }
  return;
}

1;