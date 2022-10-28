#include "duckdb/parallel/meta_pipeline.hpp"

#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"

namespace duckdb {

MetaPipeline::MetaPipeline(Executor &executor_p, PipelineBuildState &state_p, PhysicalOperator *sink_p,
                           bool preserves_order)
    : executor(executor_p), state(state_p), sink(sink_p), preserves_order(preserves_order) {
	auto base_pipeline = CreatePipeline();
	state.SetPipelineSink(*base_pipeline, sink, 0);
	if (sink_p && sink_p->type == PhysicalOperatorType::RECURSIVE_CTE) {
		recursive_cte = (PhysicalRecursiveCTE *)sink;
	}
}

Executor &MetaPipeline::GetExecutor() const {
	return executor;
}

PipelineBuildState &MetaPipeline::GetState() const {
	return state;
}

PhysicalOperator *MetaPipeline::GetSink() const {
	return sink;
}

shared_ptr<Pipeline> &MetaPipeline::GetBasePipeline() {
	return pipelines[0];
}

void MetaPipeline::GetPipelines(vector<shared_ptr<Pipeline>> &result, bool recursive) {
	result.insert(result.end(), pipelines.begin(), pipelines.end());
	if (recursive) {
		for (auto &child : children) {
			child->GetPipelines(result, true);
		}
	}
}

void MetaPipeline::GetMetaPipelines(vector<shared_ptr<MetaPipeline>> &result, bool recursive, bool skip) {
	if (!skip) {
		result.push_back(shared_from_this());
	}
	if (recursive) {
		for (auto &child : children) {
			child->GetMetaPipelines(result, true, false);
		}
	}
}

const vector<Pipeline *> *MetaPipeline::GetDependencies(Pipeline *dependant) const {
	auto it = dependencies.find(dependant);
	if (it == dependencies.end()) {
		return nullptr;
	} else {
		return &it->second;
	}
}

bool MetaPipeline::HasRecursiveCTE() const {
	return recursive_cte != nullptr;
}

bool MetaPipeline::PreservesOrder() const {
	return preserves_order;
}

void MetaPipeline::Build(PhysicalOperator *op) {
	D_ASSERT(pipelines.size() == 1);
	D_ASSERT(children.empty());
	D_ASSERT(final_pipelines.empty());
	op->BuildPipelines(*pipelines.back(), *this);
}

void MetaPipeline::Ready() {
	for (auto &pipeline : pipelines) {
		pipeline->Ready();
	}
	for (auto &child : children) {
		child->Ready();
	}
}

void MetaPipeline::Reset(bool reset_sink) {
	if (sink && reset_sink) {
		sink->sink_state = sink->GetGlobalSinkState(executor.context);
	}
	for (auto &pipeline : pipelines) {
		for (auto &op : pipeline->GetOperators()) {
			op->op_state = op->GetGlobalOperatorState(executor.context);
		}
		pipeline->Reset();
	}
	for (auto &child : children) {
		child->Reset(true);
	}
}

MetaPipeline *MetaPipeline::CreateChildMetaPipeline(Pipeline &current, PhysicalOperator *op) {
	children.push_back(make_unique<MetaPipeline>(executor, state, op, preserves_order));
	auto child_meta_pipeline = children.back().get();
	// child MetaPipeline must finish completely before this MetaPipeline can start
	current.AddDependency(child_meta_pipeline->GetBasePipeline());
	// child meta pipeline is part of the recursive CTE too
	if (HasRecursiveCTE()) {
		child_meta_pipeline->recursive_cte = recursive_cte;
	}
	return child_meta_pipeline;
}

Pipeline *MetaPipeline::CreatePipeline() {
	pipelines.emplace_back(make_unique<Pipeline>(executor));
	return pipelines.back().get();
}

void MetaPipeline::AddDependenciesFrom(Pipeline *dependant, Pipeline *start) {
	// find 'start'
	auto it = pipelines.begin();
	for (; it->get() != start; it++) {
	}

	// collect pipelines that were created from then
	vector<Pipeline *> created_pipelines;
	for (; it != pipelines.end(); it++) {
		if (it->get() == dependant) {
			// cannot depend on itself
			continue;
		}
		created_pipelines.push_back(it->get());
	}

	// add them to the dependencies
	auto &deps = dependencies[dependant];
	deps.insert(deps.begin(), created_pipelines.begin(), created_pipelines.end());
}

Pipeline *MetaPipeline::CreateUnionPipeline(Pipeline &current) {
	if (HasRecursiveCTE()) {
		throw NotImplementedException("UNIONS are not supported in recursive CTEs yet");
	}

	// create the union pipeline
	auto union_pipeline = CreatePipeline();
	state.SetPipelineOperators(*union_pipeline, state.GetPipelineOperators(current));
	state.SetPipelineSink(*union_pipeline, sink, pipelines.size() - 1);

	// 'union_pipeline' inherits ALL dependencies of 'current' (intra- and inter-MetaPipeline)
	union_pipeline->dependencies = current.dependencies;
	auto current_inter_deps = GetDependencies(&current);
	if (current_inter_deps) {
		dependencies[union_pipeline] = *current_inter_deps;
	}

	if (sink && !sink->ParallelSink()) {
		// if the sink is not parallel, we set a dependency
		dependencies[union_pipeline].push_back(&current);
	}

	return union_pipeline;
}

void MetaPipeline::CreateChildPipeline(Pipeline &current, PhysicalOperator *op) {
	// rule 2: 'current' must be fully built (down to the source) before creating the child pipeline
	D_ASSERT(current.source);
	if (HasRecursiveCTE()) {
		throw NotImplementedException("Child pipelines are not supported in recursive CTEs yet");
	}

	// create the child pipeline
	pipelines.emplace_back(state.CreateChildPipeline(executor, current, op));
	auto child_pipeline = pipelines.back().get();

	// child pipeline has an inter-MetaPipeline depency on all pipelines that were scheduled between 'current' and now
	// (including 'current') - set them up
	AddDependenciesFrom(child_pipeline, &current);
	D_ASSERT(!GetDependencies(child_pipeline)->empty());
}

} // namespace duckdb
