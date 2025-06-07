/*
Copyright 2014 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package create

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"runtime"
	"strings"

	"github.com/spf13/cobra"
	"k8s.io/klog/v2"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/cli-runtime/pkg/resource"
	"k8s.io/client-go/dynamic"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/cmd/util/editor"
	"k8s.io/kubectl/pkg/generate"
	"k8s.io/kubectl/pkg/rawhttp"
	"k8s.io/kubectl/pkg/scheme"
	"k8s.io/kubectl/pkg/util"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/templates"
)

// CreateOptions is the commandline options for 'create' sub command
type CreateOptions struct {
	PrintFlags  *genericclioptions.PrintFlags
	RecordFlags *genericclioptions.RecordFlags

	DryRunStrategy cmdutil.DryRunStrategy
	DryRunVerifier *resource.DryRunVerifier

	fieldManager string

	FilenameOptions  resource.FilenameOptions
	Selector         string
	EditBeforeCreate bool
	Raw              string

	Recorder genericclioptions.Recorder
	PrintObj func(obj kruntime.Object) error

	genericclioptions.IOStreams
}

var (
	createLong = templates.LongDesc(i18n.T(`
		Create a resource from a file or from stdin.

		JSON and YAML formats are accepted.`))

	createExample = templates.Examples(i18n.T(`
		# Create a pod using the data in pod.json.
		kubectl create -f ./pod.json

		# Create a pod based on the JSON passed into stdin.
		cat pod.json | kubectl create -f -

		# Edit the data in docker-registry.yaml in JSON then create the resource using the edited data.
		kubectl create -f docker-registry.yaml --edit -o json`))
)

// NewCreateOptions returns an initialized CreateOptions instance
func NewCreateOptions(ioStreams genericclioptions.IOStreams) *CreateOptions {
	return &CreateOptions{
		PrintFlags:  genericclioptions.NewPrintFlags("created").WithTypeSetter(scheme.Scheme),
		RecordFlags: genericclioptions.NewRecordFlags(),

		Recorder: genericclioptions.NoopRecorder{},

		IOStreams: ioStreams,
	}
}

/*
	这段代码定义了 kubectl create 子命令的实现逻辑，是整个 kubectl 命令行工具的一部分。
	创建并初始化 kubectl create 命令及其子命令（如 create deployment、create secret 等），并配置其参数、校验、执行逻辑。
*/
// NewCmdCreate returns new initialized instance of create sub command
func NewCmdCreate(f cmdutil.Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	// 初始化命令的选项对象 o，用于保存命令行参数，比如 -f、--selector 等
	o := NewCreateOptions(ioStreams)

	// 创建 cobra.Command
	// 定义 create 命令的用法（Usage）、简要描述（Short）、完整说明（Long）和示例（Example）。
	// 最重要的是 Run 字段，它定义了用户执行 kubectl create ... 时的主逻辑。
	cmd := &cobra.Command{
		Use:                   "create -f FILENAME",
		DisableFlagsInUseLine: true,
		Short:                 i18n.T("Create a resource from a file or from stdin."),
		Long:                  createLong,
		Example:               createExample,
		// 验证参数并运行
		Run: func(cmd *cobra.Command, args []string) {
			// 如果用户没有指定 -f 或 -k 参数，就打印错误并提示用法。
			if cmdutil.IsFilenameSliceEmpty(o.FilenameOptions.Filenames, o.FilenameOptions.Kustomize) {
				ioStreams.ErrOut.Write([]byte("Error: must specify one of -f and -k\n\n"))
				defaultRunFunc := cmdutil.DefaultSubCommandRun(ioStreams.ErrOut)
				defaultRunFunc(cmd, args)
				return
			}
			// 补全选项，比如解析文件名
			cmdutil.CheckErr(o.Complete(f, cmd))
			// 参数校验
			cmdutil.CheckErr(o.ValidateArgs(cmd, args))
			// 核心的运行代码逻辑是在这里的RunCreate
			cmdutil.CheckErr(o.RunCreate(f, cmd))
		},
	}

	// bind flag structs
	o.RecordFlags.AddFlags(cmd)

	usage := "to use to create the resource"
	// 这里注册各种支持的参数（flags）：
	// -f：指定 YAML 文件
	// --edit：创建前是否允许编辑资源
	// --selector：通过 label 过滤
	// --raw：指定要 POST 的原始 URI
	// --dry-run：是否只模拟不执行
	// --field-manager：用于 Server-Side Apply 的标识
	cmdutil.AddFilenameOptionFlags(cmd, &o.FilenameOptions, usage)
	cmdutil.AddValidateFlags(cmd)
	cmd.Flags().BoolVar(&o.EditBeforeCreate, "edit", o.EditBeforeCreate, "Edit the API resource before creating")
	cmd.Flags().Bool("windows-line-endings", runtime.GOOS == "windows",
		"Only relevant if --edit=true. Defaults to the line ending native to your platform.")
	cmdutil.AddApplyAnnotationFlags(cmd)
	cmdutil.AddDryRunFlag(cmd)
	cmd.Flags().StringVarP(&o.Selector, "selector", "l", o.Selector, "Selector (label query) to filter on, supports '=', '==', and '!='.(e.g. -l key1=value1,key2=value2)")
	cmd.Flags().StringVar(&o.Raw, "raw", o.Raw, "Raw URI to POST to the server.  Uses the transport specified by the kubeconfig file.")
	cmdutil.AddFieldManagerFlagVar(cmd, &o.fieldManager, "kubectl-create")

	o.PrintFlags.AddFlags(cmd)

	// 这一步将所有 create 子命令挂载上来，例如：
	// kubectl create namespace
	// kubectl create deployment
	// kubectl create secret
	// ...
	// 每一个都是一个独立的 *cobra.Command 对象
	// create subcommands
	cmd.AddCommand(NewCmdCreateNamespace(f, ioStreams))
	cmd.AddCommand(NewCmdCreateQuota(f, ioStreams))
	cmd.AddCommand(NewCmdCreateSecret(f, ioStreams))
	cmd.AddCommand(NewCmdCreateConfigMap(f, ioStreams))
	cmd.AddCommand(NewCmdCreateServiceAccount(f, ioStreams))
	cmd.AddCommand(NewCmdCreateService(f, ioStreams))
	cmd.AddCommand(NewCmdCreateDeployment(f, ioStreams))
	cmd.AddCommand(NewCmdCreateClusterRole(f, ioStreams))
	cmd.AddCommand(NewCmdCreateClusterRoleBinding(f, ioStreams))
	cmd.AddCommand(NewCmdCreateRole(f, ioStreams))
	cmd.AddCommand(NewCmdCreateRoleBinding(f, ioStreams))
	cmd.AddCommand(NewCmdCreatePodDisruptionBudget(f, ioStreams))
	cmd.AddCommand(NewCmdCreatePriorityClass(f, ioStreams))
	cmd.AddCommand(NewCmdCreateJob(f, ioStreams))
	cmd.AddCommand(NewCmdCreateCronJob(f, ioStreams))
	return cmd
}

// ValidateArgs makes sure there is no discrepency in command options
func (o *CreateOptions) ValidateArgs(cmd *cobra.Command, args []string) error {
	if len(args) != 0 {
		return cmdutil.UsageErrorf(cmd, "Unexpected args: %v", args)
	}
	if len(o.Raw) > 0 {
		if o.EditBeforeCreate {
			return cmdutil.UsageErrorf(cmd, "--raw and --edit are mutually exclusive")
		}
		if len(o.FilenameOptions.Filenames) != 1 {
			return cmdutil.UsageErrorf(cmd, "--raw can only use a single local file or stdin")
		}
		if strings.Index(o.FilenameOptions.Filenames[0], "http://") == 0 || strings.Index(o.FilenameOptions.Filenames[0], "https://") == 0 {
			return cmdutil.UsageErrorf(cmd, "--raw cannot read from a url")
		}
		if o.FilenameOptions.Recursive {
			return cmdutil.UsageErrorf(cmd, "--raw and --recursive are mutually exclusive")
		}
		if len(o.Selector) > 0 {
			return cmdutil.UsageErrorf(cmd, "--raw and --selector (-l) are mutually exclusive")
		}
		if len(cmdutil.GetFlagString(cmd, "output")) > 0 {
			return cmdutil.UsageErrorf(cmd, "--raw and --output are mutually exclusive")
		}
		if _, err := url.ParseRequestURI(o.Raw); err != nil {
			return cmdutil.UsageErrorf(cmd, "--raw must be a valid URL path: %v", err)
		}
	}

	return nil
}

// Complete completes all the required options
func (o *CreateOptions) Complete(f cmdutil.Factory, cmd *cobra.Command) error {
	var err error
	o.RecordFlags.Complete(cmd)
	o.Recorder, err = o.RecordFlags.ToRecorder()
	if err != nil {
		return err
	}

	o.DryRunStrategy, err = cmdutil.GetDryRunStrategy(cmd)
	if err != nil {
		return err
	}
	cmdutil.PrintFlagsWithDryRunStrategy(o.PrintFlags, o.DryRunStrategy)
	dynamicClient, err := f.DynamicClient()
	if err != nil {
		return err
	}
	discoveryClient, err := f.ToDiscoveryClient()
	if err != nil {
		return err
	}
	o.DryRunVerifier = resource.NewDryRunVerifier(dynamicClient, discoveryClient)

	printer, err := o.PrintFlags.ToPrinter()
	if err != nil {
		return err
	}

	o.PrintObj = func(obj kruntime.Object) error {
		return printer.PrintObj(obj, o.Out)
	}

	return nil
}

// RunCreate() 是 kubectl create 真正和 kube-apiserver 通信的代码，它会解析文件中的资源清单，逐个通过 REST API 发送 POST 请求创建对象，并支持 raw/http、dry-run、edit 模式。
// RunCreate performs the creation
func (o *CreateOptions) RunCreate(f cmdutil.Factory, cmd *cobra.Command) error {
	// 如果用户使用 --raw 参数（低级 HTTP 方式创建资源），就直接发 HTTP 请求，不走常规资源创建流程
	// raw only makes sense for a single file resource multiple objects aren't likely to do what you want.
	// the validator enforces this, so
	if len(o.Raw) > 0 {
		restClient, err := f.RESTClient()
		if err != nil {
			return err
		}
		return rawhttp.RawPost(restClient, o.IOStreams, o.Raw, o.FilenameOptions.Filenames[0])
	}

	// 如果开启 --edit，在真正创建前会打开一个编辑器让你先编辑一下资源内容
	if o.EditBeforeCreate {
		return RunEditOnCreate(f, o.PrintFlags, o.RecordFlags, o.IOStreams, cmd, &o.FilenameOptions, o.fieldManager)
	}
	// 获取 schema 验证器用于校验资源格式
	schema, err := f.Validator(cmdutil.GetFlagBool(cmd, "validate"))
	if err != nil {
		return err
	}

	// 获取当前 kubeconfig 中的命名空间，用于确定资源要创建在哪个 namespace 中
	cmdNamespace, enforceNamespace, err := f.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return err
	}

	// 使用 Builder 构建资源对象集合，它做的事包括：
	// • 解析你提供的文件 -f xxx.yaml（也支持 URL 或 stdin）；
	// • 将这些内容转换成 Kubernetes 的 资源对象集合（可以是多个）；
	// • 构建一个 Result，你可以用 Visit 遍历每个资源对象。
	r := f.NewBuilder().
		// 我们可以在Unstructured()中看到getClient函数，与kube-apiserver交互的Client在这个函数中被创建
		Unstructured().
		Schema(schema).
		ContinueOnError().
		NamespaceParam(cmdNamespace).DefaultNamespace().
		// 读取文件信息，发现除了支持简单的本地文件，也支持标准输入和http/https协议访问的文件，保存为Visitor
		FilenameParam(enforceNamespace, &o.FilenameOptions).
		LabelSelectorParam(o.Selector).
		Flatten().
		Do()
	err = r.Err()
	if err != nil {
		return err
	}

	count := 0

	//  遍历每个资源对象，逐一创建
	// • 遍历上一步构造出的资源对象；
	// • 为每个对象调用 Create(...) 发起 REST 请求；
	// • 创建成功后可以 info.Refresh() 读取创建返回的最新对象；
	// • 如果有设置 --dry-run，就只模拟发送，不会真正创建；
	err = r.Visit(func(info *resource.Info, err error) error {
		if err != nil {
			return err
		}
		if err := util.CreateOrUpdateAnnotation(cmdutil.GetFlagBool(cmd, cmdutil.ApplyAnnotationsFlag), info.Object, scheme.DefaultJSONEncoder()); err != nil {
			return cmdutil.AddSourceToErr("creating", info.Source, err)
		}

		if err := o.Recorder.Record(info.Object); err != nil {
			klog.V(4).Infof("error recording current command: %v", err)
		}

		if o.DryRunStrategy != cmdutil.DryRunClient {
			if o.DryRunStrategy == cmdutil.DryRunServer {
				if err := o.DryRunVerifier.HasSupport(info.Mapping.GroupVersionKind); err != nil {
					return cmdutil.AddSourceToErr("creating", info.Source, err)
				}
			}
			// 关键的发送函数
			obj, err := resource.
				NewHelper(info.Client, info.Mapping).
				DryRun(o.DryRunStrategy == cmdutil.DryRunServer).
				WithFieldManager(o.fieldManager).
				Create(info.Namespace, true, info.Object)
			if err != nil {
				return cmdutil.AddSourceToErr("creating", info.Source, err)
			}
			info.Refresh(obj, true)
		}

		count++

		// 打印每个创建成功的对象信息；
		return o.PrintObj(info.Object)
	})
	if err != nil {
		return err
	}
	if count == 0 {
		return fmt.Errorf("no objects passed to create")
	}
	return nil
}

// RunEditOnCreate performs edit on creation
func RunEditOnCreate(f cmdutil.Factory, printFlags *genericclioptions.PrintFlags, recordFlags *genericclioptions.RecordFlags, ioStreams genericclioptions.IOStreams, cmd *cobra.Command, options *resource.FilenameOptions, fieldManager string) error {
	editOptions := editor.NewEditOptions(editor.EditBeforeCreateMode, ioStreams)
	editOptions.FilenameOptions = *options
	editOptions.ValidateOptions = cmdutil.ValidateOptions{
		EnableValidation: cmdutil.GetFlagBool(cmd, "validate"),
	}
	editOptions.PrintFlags = printFlags
	editOptions.ApplyAnnotation = cmdutil.GetFlagBool(cmd, cmdutil.ApplyAnnotationsFlag)
	editOptions.RecordFlags = recordFlags
	editOptions.FieldManager = "kubectl-create"

	err := editOptions.Complete(f, []string{}, cmd)
	if err != nil {
		return err
	}
	return editOptions.Run()
}

// NameFromCommandArgs is a utility function for commands that assume the first argument is a resource name
func NameFromCommandArgs(cmd *cobra.Command, args []string) (string, error) {
	argsLen := cmd.ArgsLenAtDash()
	// ArgsLenAtDash returns -1 when -- was not specified
	if argsLen == -1 {
		argsLen = len(args)
	}
	if argsLen != 1 {
		return "", cmdutil.UsageErrorf(cmd, "exactly one NAME is required, got %d", argsLen)
	}
	return args[0], nil
}

// CreateSubcommandOptions is an options struct to support create subcommands
type CreateSubcommandOptions struct {
	// PrintFlags holds options necessary for obtaining a printer
	PrintFlags *genericclioptions.PrintFlags
	// Name of resource being created
	Name string
	// StructuredGenerator is the resource generator for the object being created
	StructuredGenerator generate.StructuredGenerator
	DryRunStrategy      cmdutil.DryRunStrategy
	DryRunVerifier      *resource.DryRunVerifier
	CreateAnnotation    bool
	FieldManager        string

	Namespace        string
	EnforceNamespace bool

	Mapper        meta.RESTMapper
	DynamicClient dynamic.Interface

	PrintObj printers.ResourcePrinterFunc

	genericclioptions.IOStreams
}

// NewCreateSubcommandOptions returns initialized CreateSubcommandOptions
func NewCreateSubcommandOptions(ioStreams genericclioptions.IOStreams) *CreateSubcommandOptions {
	return &CreateSubcommandOptions{
		PrintFlags: genericclioptions.NewPrintFlags("created").WithTypeSetter(scheme.Scheme),
		IOStreams:  ioStreams,
	}
}

// Complete completes all the required options
func (o *CreateSubcommandOptions) Complete(f cmdutil.Factory, cmd *cobra.Command, args []string, generator generate.StructuredGenerator) error {
	name, err := NameFromCommandArgs(cmd, args)
	if err != nil {
		return err
	}

	o.Name = name
	o.StructuredGenerator = generator
	o.DryRunStrategy, err = cmdutil.GetDryRunStrategy(cmd)
	if err != nil {
		return err
	}
	dynamicClient, err := f.DynamicClient()
	if err != nil {
		return err
	}
	discoveryClient, err := f.ToDiscoveryClient()
	if err != nil {
		return err
	}
	o.DryRunVerifier = resource.NewDryRunVerifier(dynamicClient, discoveryClient)
	o.CreateAnnotation = cmdutil.GetFlagBool(cmd, cmdutil.ApplyAnnotationsFlag)

	cmdutil.PrintFlagsWithDryRunStrategy(o.PrintFlags, o.DryRunStrategy)
	printer, err := o.PrintFlags.ToPrinter()
	if err != nil {
		return err
	}

	o.PrintObj = func(obj kruntime.Object, out io.Writer) error {
		return printer.PrintObj(obj, out)
	}

	o.Namespace, o.EnforceNamespace, err = f.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return err
	}

	o.DynamicClient, err = f.DynamicClient()
	if err != nil {
		return err
	}

	o.Mapper, err = f.ToRESTMapper()
	if err != nil {
		return err
	}

	return nil
}

// Run executes a create subcommand using the specified options
func (o *CreateSubcommandOptions) Run() error {
	obj, err := o.StructuredGenerator.StructuredGenerate()
	if err != nil {
		return err
	}
	if err := util.CreateOrUpdateAnnotation(o.CreateAnnotation, obj, scheme.DefaultJSONEncoder()); err != nil {
		return err
	}
	if o.DryRunStrategy != cmdutil.DryRunClient {
		// create subcommands have compiled knowledge of things they create, so type them directly
		gvks, _, err := scheme.Scheme.ObjectKinds(obj)
		if err != nil {
			return err
		}
		gvk := gvks[0]
		mapping, err := o.Mapper.RESTMapping(schema.GroupKind{Group: gvk.Group, Kind: gvk.Kind}, gvk.Version)
		if err != nil {
			return err
		}

		asUnstructured := &unstructured.Unstructured{}
		if err := scheme.Scheme.Convert(obj, asUnstructured, nil); err != nil {
			return err
		}
		if mapping.Scope.Name() == meta.RESTScopeNameRoot {
			o.Namespace = ""
		}
		createOptions := metav1.CreateOptions{}
		if o.FieldManager != "" {
			createOptions.FieldManager = o.FieldManager
		}
		if o.DryRunStrategy == cmdutil.DryRunServer {
			if err := o.DryRunVerifier.HasSupport(mapping.GroupVersionKind); err != nil {
				return err
			}
			createOptions.DryRun = []string{metav1.DryRunAll}
		}
		actualObject, err := o.DynamicClient.Resource(mapping.Resource).Namespace(o.Namespace).Create(context.TODO(), asUnstructured, createOptions)
		if err != nil {
			return err
		}

		// ensure we pass a versioned object to the printer
		obj = actualObject
	} else {
		if meta, err := meta.Accessor(obj); err == nil && o.EnforceNamespace {
			meta.SetNamespace(o.Namespace)
		}
	}

	return o.PrintObj(obj, o.Out)
}
