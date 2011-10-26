package Bio::EnsEMBL::DBDumper::Sql;

use strict;
use warnings;
use base qw/Bio::EnsEMBL::DBDumper::Base/;

sub run {
  my ($self) = @_;
  my $sql = $self->_get_sql();
  my $fh = $self->fh();
  print $fh $sql, ';', "\n"x2;
  return;
}

sub _get_sql {
  my ($self) = @_;
  my ($q_table) = @{$self->dbc()->quote_identifier($self->name())};
  my $sql = $self->dbc()->sql_helper()->execute_single_result(
    -SQL => qq{SHOW CREATE TABLE $q_table},
    -CALLBACK => sub {
      my ($row) = @_;
      return $row->[1];
    }
  );
  return $self->_modify_sql($sql);
}

sub _modify_sql {
  my ($self, $sql) = @_;
  if($self->is_view()) {
    $sql =~ s/DEFINER=.+ \s+ SQL/DEFINER=CURRENT_USER() SQL/xms;
    $sql =~ s/SQL \s+ SECURITY \s+ DEFINER/SQL SECURITY INVOKER/xms;
  }
  return $sql;
}

1;