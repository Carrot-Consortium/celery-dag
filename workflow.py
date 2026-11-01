from celery import Signature, chord, current_task, maybe_signature
from celery.result import AsyncResult
from celery.utils.objects import getitem_property
from kombu.utils.uuid import uuid
from celery.app.task import Context
from celery.backends.redis import RedisBackend
from celery.backends.base import Backend


class MyTask(Task):
    def apply_async(
        self,
        args=None,
        kwargs=None,
        task_id=None,
        producer=None,
        link=None,
        link_error=None,
        shadow=None,
        **options,
    ):
        if "kwarg_mapping" in options:
            kwargs.update(
                {
                    kwarg: args[0][idx] if isinstance(idx, int) else [args[0][lidx] for lidx in idx]
                    for kwarg, idx in options["kwarg_mapping"].items()
                }
            )
            args = args[1:]
        elif isinstance(self.request.chord, list) and options.get("unpack_outputs", False):
            args = args[0] if args else ()

        async_result = super().apply_async(
            args=args,
            kwargs=kwargs,
            task_id=task_id,
            producer=producer,
            link=link,
            link_error=link_error,
            shadow=shadow,
            **options,
        )

        if "deferred" in options:
            return AsyncResult(options["workflow_output_id"])

        return async_result

    def on_replace(self, sig):

        if sig.task == "workflow":
            sig.nodes[-1].id = self.request.id            
            sig.nodes[-1].options.setdefault("group_id", self.request.group)
            sig.nodes[-1].options.setdefault("group_index", self.request.group_index)
            sig.nodes[-1].options.setdefault("chord", self.request.chord)

        if isinstance(self.request.chord, list):      
            if sig.task == "celery.chord":
                end_sig = sig.body
            elif sig.task == "celery.chain":
                end_sig = sig.tasks[-1]
            elif sig.task == "workflow":
                end_sig = sig.nodes[-1]
            else:
                end_sig = sig

            end_sig.options.setdefault("chord", self.request.chord)

        return super().on_replace(sig)

    def retry(self, args=None, kwargs=None, **options):
        if isinstance(self.request.chord, list):
            options.setdefault("chord", self.request.chord)

        return super().retry(args=args, kwargs=kwargs, **options)


app = Celery(
    "sample",
    broker="pyamqp://guest:guest@rabbit:5672//",  
    backend="redis://redis:6379/0",
    task_cls=MyTask,
)

@app.task
def add(x, y):
    return x + y


@app.task
def tsum(numbers):
    return sum(numbers)


@app.task
def ident(arg):
    return arg


orig_ocpr = RedisBackend.on_chord_part_return


def new_ocpr(self, request, state, result, propagate=None, **kwargs):
    properties = request.properties or request.options
    if isinstance(request.chord, list):
        for group, group_index, chord, _ in zip(*request.chord):
            new_request = Context(request.__dict__)       
            new_request.group = group
            new_request.group_index = group_index         
            new_request.chord = chord
            orig_ocpr(self, new_request, state, result, propagate=propagate, **kwargs) 
    else:
        orig_ocpr(self, request, state, result, propagate=propagate, **kwargs) 


RedisBackend.on_chord_part_return = new_ocpr


orig_ocpr2 = Backend.on_chord_part_return


def new_ocpr2(self, request, state, result, **kwargs):
    if isinstance(request.chord, list):
        for group, group_index, chord, siblings in zip(*request.chord):
            if 0 == siblings.index(request.id):
                self.fallback_chord_unlock(
                    GroupResult(
                        id=group,
                        results=[
                            AsyncResult(sibling) for sibling in siblings if sibling is not None
                        ],
                    ),
                    maybe_signature(chord),
                )
    else:
        orig_ocpr2(self, request, state, result, **kwargs)


Backend.on_chord_part_return = new_ocpr2


@Signature.register_type(name="workflow")
class workflow(Signature):
    tasks = getitem_property("kwargs.tasks", "Tasks in workflow.")
    g = getitem_property("kwargs.g", "Tasks graph.")

    def __init__(self, tasks=None, g=None, args=None, kwargs=None, app=None, **options): 

        self.kwargs = {}

        self.tasks = [maybe_signature(task) for task in tasks] if tasks else None
        self.nodes = self.tasks or []
        self.g = {int(k): v for k, v in g.items()} if g else {}
        self.prepared = bool(self.nodes)

        super().__init__(
            "workflow", args=args, kwargs=self.kwargs, app=app, subtask_type="workflow", **options
        )

    @classmethod
    def from_dict(cls, d, app=None):
        return cls(
            *(d.get("args") or ()),
            app=app,
            **{**(d.get("kwargs") or {}), **(d.get("options") or {})},
        )

    def freeze(self, *args, outputs=None, **options): 
        outputs = outputs or self.nodes
        self.prepare(outputs)
        for node in self.nodes:
            node.set(**{k: v for k, v in options.items() if k != "chord"}) 
        return AsyncResult(self.nodes[-1].id)

    def to_celery(self, outputs=None, **options): 
        self.freeze(outputs=outputs, **options)       
        return self

    def prepare(self, outputs=None):
        if self.prepared:
            return
        if isinstance(outputs, list):
            output_node_ids = [id(node) for node in outputs]
        elif outputs:
            output_node_ids = id(outputs)
        else:
            output_node_ids = None
        self.nodes.append(ident.s())
        if not output_node_ids:
            self.g[len(self.nodes) - 1] = {"arg": list(range(len(self.nodes) - 1))}
        else:
            node_ids = [id(node) for node in self.nodes]  
            if isinstance(output_node_ids, list):         
                self.g[len(self.nodes) - 1] = {               
                    "arg": [node_ids.index(output) for output in output_node_ids]
                }
            else:
                self.g[len(self.nodes) - 1] = {"arg": node_ids.index(output_node_ids)} 
        kwarg_map = {}
        for node_idx, parents in self.g.items():      
            parent_idxs = [
                idx
                for idxs in parents.values()
                for idx in (idxs if isinstance(idxs, list) else [idxs])
            ]
            parent_idxs = list(set(parent_idxs))          
            node_sig = self.nodes[node_idx]
            node_sig.freeze()
            header_tasks = [self.nodes[parent_idx] for parent_idx in parent_idxs]
            for task in header_tasks:
                task.freeze()
            group_id = uuid()
            for idx, parent_idx in enumerate(parent_idxs):
                kwarg_map[parent_idx] = idx
                parent_sig = self.nodes[parent_idx]           
                if isinstance(parent_sig, workflow):          
                    output_sig = parent_sig.nodes[-1]
                else:
                    output_sig = parent_sig
                output_sig.options.setdefault("chord", [[], [], [], []])[0].append(group_id)
                output_sig.options.setdefault("chord", [[], [], [], []])[1].append(idx)
                output_sig.options.setdefault("chord", [[], [], [], []])[2].append(node_sig)
                output_sig.options.setdefault("chord", [[], [], [], []])[3].append(
                    [
                        (
                            t.nodes[-1].id
                            if isinstance(t, workflow)
                            else t.body.id if getattr(t, "tasks", None) else t.id
                        )
                        for t in header_tasks
                    ]
                )
            if not node_sig.immutable and any(p for p in parents.keys() if p is not None):
                kwarg_mapping = {
                    kwarg: (
                        kwarg_map[parent_idx]
                        if isinstance(parent_idx, int)                
                        else [kwarg_map[pidx] for pidx in parent_idx] 
                    )
                    for kwarg, parent_idx in parents.items()      
                    if kwarg is not None
                }
                if header_tasks := getattr(node_sig, "tasks", None):
                    for header_sig in header_tasks.tasks:         
                        header_sig.set(kwarg_mapping=kwarg_mapping)   
                else:
                    node_sig.set(kwarg_mapping=kwarg_mapping)     
            if parent_idxs:
                node_sig.app.backend.set_chord_size(group_id, len(parent_idxs))
        for node in self.nodes:
            if node.options.get("deferred"):
                node.set(workflow_output_id=self.nodes[-1].id)
        self.tasks = self.nodes
        self.prepared = True

    def apply_async(self, args=None, kwargs=None, outputs=None, **options):
        if not self.prepared:
            self.prepare(outputs)

        options = {k: v for k, v in options.items() if k not in ("group_index", "producer")}

        self.nodes[-1].set(**options)

        if current_task:
            self.stamp(**current_task.request.stamps)     

        options = {k: v for k, v in options.items() if k not in ("chord", "group_id")}

        for idx, node in enumerate(self.nodes):       
            if idx not in self.g and not node.options.get("deferred"):
                node.apply_async(args=args, kwargs=kwargs, **options)

        return AsyncResult(self.nodes[-1].id)

    def add_node(self, sig, inputs=None, depends_on=None, unpack_outputs=False):
        if sig.task == "celery.group":
            sig = chord(sig.tasks, ident.s())

        self.nodes.append(sig)
        node_ids = [id(node) for node in self.nodes]  
        if inputs:
            sig.immutable = False
            self.g[len(self.nodes) - 1] = {
                kwarg: (
                    node_ids.index(id(parent_sig))                
                    if not isinstance(parent_sig, list)           
                    else [node_ids.index(id(psig)) for psig in parent_sig]
                )
                for kwarg, parent_sig in inputs.items()       
            }
        if depends_on:
            self.g.setdefault(len(self.nodes) - 1, {})[None] = [
                node_ids.index(dep_id)
                for dep_id in set(id(parent_sig) for parent_sig in depends_on)
            ]
        if unpack_outputs:
            sig.set(unpack_outputs=True)
        return sig

    def add_deferred_input(self):
        node = self.add_node(ident.s())
        node.options.setdefault("deferred", True)     
        return node

    def stamp(self, visitor=None, append_stamps=False, **headers):
        for node in self.nodes:
            node.stamp(visitor=visitor, append_stamps=append_stamps, **headers)
        return super().stamp(visitor=visitor, append_stamps=append_stamps, **headers)

    subtask_type = "workflow"
