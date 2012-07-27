package Bio::EnsEMBL::DBLoader::RunnableDB::DatabaseFactory;

use strict;
use warnings;
use base qw/Bio::EnsEMBL::Hive::RunnableDB::JobFactory Bio::EnsEMBL::DBLoader::RunnableDB::Base/;

use Bio::EnsEMBL::Utils::Exception qw/throw/;
use Bio::EnsEMBL::Utils::Scalar qw/scope_guard/;

my %allowed_modes = map { $_ => 1 } qw/mart ensembl all/;
my %filters = (
  mart => sub {
    my ($self, $dir) = @_;
    return $dir =~ /_mart_/ ? 1 : 0;
  },
  ensembl => sub {
    my ($self, $dir) = @_;
    return $dir !~ /_mart_/ ? 1 : 0;
  },
  all => sub {
    my ($self, $dir) = @_;
    my $databases = $self->param('databases');
    if($databases) {
      my %lookup = map { $_ => 1 } @{$databases};
      if($lookup{$dir}) {
        $DB::single=1;;
        $self->param('hardcoded_db_hits')->{$dir} = 1;
        return 1;
      } 
      return 0;
    }
    return 1;
  },
);

sub param_defaults {
  my ($self) = @_;
  return {
    mode => 'all',
    hardcoded_db_hits => {},
  };
}

sub fetch_input {
  my ($self) = @_;
  my $mode = $self->param('mode') || q{};
  if(! exists $allowed_modes{$mode}) {
    my $join = join(q{, }, sort keys %allowed_modes);
    throw(sprintf(q{Mode '%s' is not a supported mode. We support [%s]}, $mode, $join));
  }
  
  my $databases = $self->param('databases');
  if($databases) {
    if(!ref($databases) || ref($databases) ne 'ARRAY') {
      $databases = [$databases];
      $self->param('databases', $databases);
    }
    #Just make sure we are asserting we have some kind of input (defaults to an array from the CFG)
    if(@{$databases}) {
      if($self->param('mode') ne 'all') {
        my $dbs = join(q{, }, @{$databases});
        throw "Cannot continue. You have requested to load '$dbs' but you are in mode '$mode'. Switch mode to 'all'";
      }
    }
  }
  
  my $dirs = $self->dirs();
  $self->param('inputlist', $dirs);
  
  #Check we loaded all DBs we asked to
  $self->assert_hardcoded_dbs();
  
  return;
}

sub dirs {
  my ($self) = @_;
  my $ftp = $self->connect_ftp();
  my $guard = scope_guard(sub { $self->disconnect_ftp(); });
  
  my $base_directory = $self->base_ftp_path();
  $self->cwd_ftp_dir($base_directory);
  
  my $ls = $self->ls_ftp_cwd();
  my $dirs = $ls->{dirs};
  my $filter = $filters{$self->param('mode')};
  my @ok_dirs;
  foreach my $dir (@{$dirs}) {
    if($filter->($self, $dir)) {
      push(@ok_dirs, $dir);
    }
  }
  return \@ok_dirs;
}

sub assert_hardcoded_dbs {
  my ($self) = @_;
  my $databases = $self->param('databases');
  my @missed;
  foreach my $db (@{$databases}) {
    if(! $self->param('hardcoded_db_hits')->{$db}) {
      push(@missed, $db);
    }
  }
  if(@missed) {
    my $j = join(q{, }, @missed);
    throw "Could not find the following databases on the remote server [$j]";
  }
  return;
}

1;