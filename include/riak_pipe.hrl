-record(fitting_details,
        {
          fitting,
          name,
          module,
          arg,
          output,
          options
        }).

-record(fitting,
        {
          pid,
          ref,
          partfun
        }).

-record(fitting_spec,
        {
          name,
          module,
          arg,
          partfun
        }).

-record(pipe_result,
        {
          ref,
          from,
          result
        }).

-record(pipe_eoi,
        {
          ref
        }).

-record(pipe_log,
        {
          ref,
          from,
          msg
        }).
