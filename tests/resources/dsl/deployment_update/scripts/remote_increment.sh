#!/bin/bash

# load current ops count (or default to 0)
if [[ "$(ctx type)" == 'relationship-instance' ]]
then
remote_ops_counter=$(ctx target instance runtime_properties 'remote_ops_counter')
else
remote_ops_counter=$(ctx instance runtime_properties 'remote_ops_counter')
fi
remote_ops_counter=${remote_ops_counter:-0}

# increment ops count
remote_ops_counter=$(expr $remote_ops_counter + 1)

# store updated count
if [[ "$(ctx type)" == 'relationship-instance' ]]
then
ctx target instance runtime_properties 'remote_ops_counter' $remote_ops_counter
else
ctx instance runtime_properties 'remote_ops_counter' $remote_ops_counter
fi