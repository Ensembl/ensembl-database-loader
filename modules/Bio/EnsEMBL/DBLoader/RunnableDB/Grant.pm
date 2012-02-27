package Bio::EnsEMBL::DBLoader::RunnableDB::FindDbs;

use strict;
use warnings;
use base qw/Bio::EnsEMBL::DBLoader::RunnableDB::Base Bio::EnsEMBL::DBLoader::RunnableDB::Database/;

sub param_defaults {
  my ($self) = @_;
  return {
    users => {
      ro => [qw/anonymous ensro/],
      rw => [qw/ensrw admin ensadmin root/],
    },
    privilages => {
      ro => [qw/SELECT/],
      rw => [qw/ALL/]
    }
  };
}

sub fetch_input {
  my ($self) = @_;
  #Switch to current schema
  $self->switch_db($self->database());
  #Process tables
  my $all_tables = $self->_tables();
  my $tables = [grep { $self->is_view($_) } @{$all_tables}];
  $self->param('tables', $tables);
  return;
}

sub run {
  my ($self) = @_;
  #Grant away
  foreach my $table (@{$self->param('tables')}) {
    foreach my $group (keys %{$self->param('users')}) {
      my $users = $self->param('users')->{$group};
      foreach my $users (@{$users}) {
        $self->_grant($table, $group, $user);
      }
    }
  }
  return;
}

### PRIVATE

sub _tables {
  my ($self) = @_;
  my $sql = 'select TABLE_NAME from information_schema.TABLES';
  return $self->sql_helper()->execute_simple(-SQL => $sql);
}

sub _grant {
  my ($self, $table, $group, $user) = @_;
  my $privilages = $self->param('privilages')->{$group};
  my $p_str = join(q{, }, @{$privilages});
  if($self->debug()) {
    printf(STDERR "Granting [%s] to %s on table %s\n", $p_str, $user, $table);
  }
  my $grant = sprintf(q{GRANT %s on %s to '%s'@'%'}, $p_str, $table, $user);
  $self->dbc()->do($grant);
  return;
}

1;