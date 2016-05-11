-module (mondemand_annotationmsg).

-include ("mondemand_internal.hrl").
-include_lib ("lwes/include/lwes.hrl").

-export ([ new/6, % (Id, Timestamp, Description, Text, [Tag], [{ContextKey,ContextValue}])
           id/1,
           timestamp/1,
           text/1,
           description/1,
           tags/1,
           context/1,
           context_value/2,
           to_lwes/1,
           from_lwes/1
         ]).

new (Id, Time, Description, Text, Tags, Context) ->
  ValidatedContext = mondemand_util:binaryify_context (Context),
  #md_annotation_msg { id = Id,
                       timestamp = validate_time(Time),
                       text = Text,
                       description = Description,
                       num_tags = length(Tags),
                       tags = Tags,
                       num_context = length (ValidatedContext),
                       context = ValidatedContext
                     }.

validate_time (Timestamp = {_,_,_}) ->
  mondemand_util:now_to_epoch_millis (Timestamp);
validate_time (Timestamp) when is_integer (Timestamp) ->
  Timestamp.

id (#md_annotation_msg { id = Id }) -> Id.
timestamp (#md_annotation_msg { timestamp = Timestamp }) -> Timestamp.
text (#md_annotation_msg { text = Text }) -> Text.
description (#md_annotation_msg { description = Description }) -> Description.
tags (#md_annotation_msg { tags = Tags }) -> Tags.
context (#md_annotation_msg { context = Context }) -> Context.
context_value (#md_annotation_msg { context = Context }, ContextKey) ->
  context_find (ContextKey, Context, undefined).

context_find (Key, Context, Default) ->
  case lists:keyfind (Key, 1, Context) of
    false -> Default;
    {_, H} -> H
  end.

tags_from_lwes (Data) ->
  Num = mondemand_util:find_in_dict (?MD_ANNOTATION_TAG_NUM, Data, 0),
  { Num,
    [ dict:fetch (annotation_tag_key (TagIndex), Data)
      || TagIndex <- lists:seq (1, Num)
    ]
  }.

tags_to_lwes (NumTags, Tags) ->
  [ { ?LWES_U_INT_16, ?MD_ANNOTATION_TAG_NUM, NumTags }
      | lists:zipwith (fun tag_to_lwes/2, lists:seq (1, NumTags), Tags)
  ].

tag_to_lwes (TagIndex, Tag) ->
  { ?LWES_STRING, annotation_tag_key (TagIndex), Tag }.

to_lwes (L) when is_list (L) ->
  lists:map (fun to_lwes/1, L);
to_lwes (#md_annotation_msg { id = Id,
                              timestamp = Time,
                              text = Text,
                              description = Description,
                              num_tags = NumTags,
                              tags = Tags,
                              num_context = NumContexts,
                              context = Context
                            }) ->
  #lwes_event {
    name = ?MD_ANNOTATION_EVENT,
    attrs = lists:flatten ([ { ?LWES_STRING, ?MD_ANNOTATION_ID, Id },
                             { ?LWES_U_INT_64, ?MD_ANNOTATION_TIMESTAMP, Time },
                             { ?LWES_STRING, ?MD_ANNOTATION_TEXT, Text},
                             { ?LWES_STRING, ?MD_ANNOTATION_DESCRIPTION, Description },
                             tags_to_lwes (NumTags, Tags),
                             mondemand_util:context_to_lwes (undefined,
                                                             NumContexts,
                                                             Context)
                           ])
  }.

from_lwes (#lwes_event { attrs = Data}) ->
  {NumTags, Tags} = tags_from_lwes (Data),
  {_, NumContexts, Context} = mondemand_util:context_from_lwes (Data),
  ReceiptTime =
    case dict:find (?MD_RECEIPT_TIME, Data) of
      error -> 0;
      {ok, RT} -> RT
    end,
  { ReceiptTime,
    #md_annotation_msg {
      id = dict:fetch (?MD_ANNOTATION_ID, Data),
      timestamp = dict:fetch (?MD_ANNOTATION_TIMESTAMP, Data),
      text = dict:fetch (?MD_ANNOTATION_TEXT, Data),
      description = dict:fetch (?MD_ANNOTATION_DESCRIPTION, Data),
      num_tags = NumTags,
      tags = Tags,
      num_context = NumContexts,
      context = Context
    }
  }.

% Precompute tag keys
annotation_tag_key (N) ->
  ?ELEMENT_OF_TUPLE_LIST (N, ?MD_ANNOTATION_TAG).
