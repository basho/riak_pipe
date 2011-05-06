-record(fitting,
        {
          pid :: pid(),
          ref :: reference(),
          partfun :: partfun()
        }).

-record(fitting_details,
        {
          fitting :: #fitting{},
          name :: term(),
          module :: atom(),
          arg :: term(),
          output :: #fitting{},
          options :: exec_opts()
        }).

-record(fitting_spec,
        {
          name :: term(),
          module :: atom(),
          arg :: term(),
          partfun :: partfun()
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

-type ring_idx() :: integer(). % ring partition index
-type partfun() :: fun((term()) -> ring_idx()) | follow | sink.
-type exec_opts() :: [exec_option()].
-type exec_option() :: {sink, #fitting{pid :: pid()}}
                     | {trace, trace_option()}
                     | {log, log_option()}.
-type trace_option() :: all
                      | list()
                      | set().
-type log_option() :: sink
                    | sasl.
