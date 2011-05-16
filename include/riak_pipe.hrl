-record(fitting,
        {
          pid :: pid(),
          ref :: reference(),
          chashfun :: riak_pipe_vnode:chashfun()
        }).

-record(fitting_details,
        {
          fitting :: #fitting{},
          name :: term(),
          module :: atom(),
          arg :: term(),
          output :: #fitting{},
          options :: riak_pipe:exec_opts()
        }).

-record(fitting_spec,
        {
          name :: term(),
          module :: atom(),
          arg :: term(),
          chashfun :: riak_pipe_vnode:chashfun()
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
